from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
import random

def create_spark_session():
    """创建并配置SparkSession，确保连接到Hive"""
    return SparkSession.builder \
        .appName("ECommerceMarketingDashboard") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("hive.metastore.uris", "thrift://your-hive-metastore-host:9083") \
    .config("spark.python.worker.reuse", "true") \
        .config("spark.python.profile", "false") \
        .config("spark.python.worker.timeout", "120s") \
        .enableHiveSupport() \
        .getOrCreate()

def generate_mock_data(spark, bizdate, record_count=100000):
    """生成模拟业务数据并写入Hive"""
    print(f"开始生成{record_count}条模拟数据，业务日期: {bizdate}")

    # 1. 生成店铺数据
    shops = spark.createDataFrame([
        (f"shop{i:03d}", f"{['旗舰','数码','家居','服饰','食品','美妆','母婴','图书','运动','家电'][i%10]}店",
         random.randint(1, 3), random.randint(1, 2), f"user{i:03d}")
        for i in range(10)
    ], ["shop_id", "shop_name", "shop_level", "shop_type", "owner_id"])

    # 2. 生成商品数据
    product_data = [
        (f"p{i:04d}",
         f"{random.choice(['笔记本','手机','电视','冰箱','沙发'])}{i%100}",
         f"shop{random.randint(0, 9):03d}",
         f"cate{random.randint(1, 5):02d}",
         random.choice(["电子产品", "家电", "家具", "服饰", "食品"]),
         float(round(random.uniform(100, 10000), 2)),
         1,
         random.random() > 0.8,
         random.random() > 0.9)
        for i in range(1000)
    ]

    products = spark.createDataFrame(product_data, [
        "product_id", "product_name", "shop_id", "category_id", "category_name",
        "price", "status", "is_pre_sale", "is_billion_subsidy"
    ])

    # 3. 生成优惠记录
    records = spark.range(record_count).select(
        expr("concat('rec', format_string('%07d', id))").alias("record_id"),
        expr("concat('act', format_string('%03d', cast(rand()*50 as int)))").alias("activity_id"),
        expr("concat('p', format_string('%04d', cast(rand()*1000 as int)))").alias("product_id"),
        when(rand() > 0.3, expr("concat('sku', cast(rand()*10 as int))")).otherwise(None).alias("sku_id"),
        expr("concat('buyer', format_string('%05d', cast(rand()*100000 as int)))").alias("buyer_id"),
        expr("concat('cs', format_string('%03d', cast(rand()*100 as int)))").alias("cs_id"),
        (rand() * 200 + 5).cast("decimal(10,2)").alias("discount_amount"),
        lit(24).alias("valid_hours"),
        current_timestamp().alias("create_time"),
        when(rand() > 0.4, 1).otherwise(when(rand() > 0.5, 0).otherwise(2)).alias("status"),
        when(rand() > 0.4, expr("concat('order', cast(rand()*900000+100000 as int))")).otherwise(None).alias("order_id"),
        when(rand() > 0.7, array(lit("价格敏感客户"), lit("老客户维护"), lit("新客转化"), lit("投诉补偿"))).otherwise(None).alias("remark")
    )

    # 写入Hive ODS层
    shops.withColumn("dt", lit(bizdate)).write.mode("overwrite").insertInto("ods_shop")
    products.withColumn("dt", lit(bizdate)).write.mode("overwrite").insertInto("ods_product")
    records.withColumn("dt", lit(bizdate)).write.mode("overwrite").insertInto("ods_marketing_cs_discount_records")

def process_dim_layer(spark, bizdate):
    """处理DIM层数据（含SCD Type2逻辑）并写入Hive"""
    print(f"\nProcessing DIM layer for {bizdate}")

    # 店铺维度处理
    spark.sql(f"""
    INSERT OVERWRITE TABLE dim_shop PARTITION(dt='{bizdate}')
    SELECT 
        s.shop_id, s.shop_name, s.shop_level, s.shop_type, s.owner_id,
        current_timestamp() as create_time, current_timestamp() as update_time,
        '{bizdate}' as start_date, '99991231' as end_date, true as is_current
    FROM ods_shop s
    WHERE s.dt = '{bizdate}'
    """)

    # 商品维度处理（SCD Type2）
    spark.sql(f"""
    -- Step 1: Expire changed records
    INSERT OVERWRITE TABLE dim_product PARTITION(dt='{bizdate}')
    SELECT 
        d.product_id, d.product_name, d.shop_id, d.category_id, d.category_name,
        d.price, d.status, d.is_pre_sale, d.is_billion_subsidy,
        d.create_time, current_timestamp() as update_time,
        d.start_date, '{bizdate}' as end_date, false as is_current
    FROM dim_product d
    JOIN ods_product o ON d.product_id = o.product_id AND o.dt = '{bizdate}'
    WHERE d.is_current AND d.dt = (
        SELECT MAX(dt) FROM dim_product WHERE product_id = d.product_id
    )
    AND (
        d.price != o.price OR 
        d.status != o.status OR 
        d.is_pre_sale != o.is_pre_sale OR
        d.is_billion_subsidy != o.is_billion_subsidy
    )
    
    UNION ALL
    
    -- Step 2: Insert new records
    SELECT 
        o.product_id, o.product_name, o.shop_id, o.category_id, o.category_name,
        o.price, o.status, o.is_pre_sale, o.is_billion_subsidy,
        current_timestamp() as create_time, current_timestamp() as update_time,
        '{bizdate}' as start_date, '99991231' as end_date, true as is_current
    FROM ods_product o
    WHERE o.dt = '{bizdate}'
    """)

def process_dwd_layer(spark, bizdate):
    """处理DWD层明细事实表并写入Hive"""
    print(f"\nProcessing DWD layer for {bizdate}")

    spark.sql(f"""
    INSERT OVERWRITE TABLE dwd_cs_discount_fact PARTITION(dt='{bizdate}')
    SELECT 
        r.record_id, r.activity_id, 
        p.product_id, p.product_name, p.price as original_price,
        r.discount_amount, 
        (p.price - r.discount_amount) as final_price,
        r.buyer_id, r.cs_id, 
        r.create_time, 
        date_add(r.create_time, r.valid_hours/24) as expire_time,
        CASE 
            WHEN r.status = 1 AND date_add(r.create_time, r.valid_hours/24) >= current_timestamp() THEN 'used'
            WHEN r.status = 1 THEN 'expired_after_use'
            WHEN date_add(r.create_time, r.valid_hours/24) < current_timestamp() THEN 'expired'
            ELSE 'unused'
        END as discount_status,
        r.order_id,
        date_format(r.create_time, 'yyyyMMdd') as create_date
    FROM ods_marketing_cs_discount_records r
    JOIN ods_product p ON r.product_id = p.product_id AND p.dt = '{bizdate}'
    WHERE r.dt = '{bizdate}'
    """)

def process_dws_layer(spark, bizdate):
    """处理DWS层汇总数据并写入Hive"""
    print(f"\nProcessing DWS layer for {bizdate}")

    spark.sql(f"""
    INSERT OVERWRITE TABLE dws_discount_daily PARTITION(dt='{bizdate}')
    SELECT 
        create_date,
        product_id,
        product_name,
        COUNT(DISTINCT record_id) as send_count,
        SUM(CASE WHEN discount_status = 'used' THEN 1 ELSE 0 END) as use_count,
        COUNT(DISTINCT CASE WHEN discount_status = 'used' THEN buyer_id END) as buyer_count,
        SUM(discount_amount) as total_discount,
        SUM(final_price) as gmv,
        original_price,
        AVG(discount_amount) as avg_discount
    FROM dwd_cs_discount_fact
    WHERE dt = '{bizdate}'
    GROUP BY create_date, product_id, product_name, original_price
    """)

def process_ads_layer(spark, bizdate):
    """处理ADS层应用数据并写入Hive"""
    print(f"\nProcessing ADS layer for {bizdate}")

    spark.sql(f"""
    INSERT OVERWRITE TABLE ads_discount_kpi PARTITION(dt='{bizdate}')
    SELECT 
        create_date as stat_date,
        'all' as dimension,
        'total' as dimension_value,
        SUM(send_count) as send_count,
        SUM(use_count) as use_count,
        ROUND(SUM(use_count)/NULLIF(SUM(send_count), 0), 4) as use_rate,
        SUM(total_discount) as total_discount,
        SUM(gmv) as gmv,
        ROUND(SUM(total_discount)/NULLIF(SUM(gmv), 0), 4) as discount_ratio
    FROM dws_discount_daily
    WHERE dt = '{bizdate}'
    GROUP BY create_date
    """)

def main():
    """主执行流程"""
    spark = None
    try:
        spark = create_spark_session()
        bizdate = datetime.datetime.now().strftime("%Y%m%d")

        # 生成模拟数据
        generate_mock_data(spark, bizdate, 100000)

        # 处理各层数据
        process_dim_layer(spark, bizdate)
        process_dwd_layer(spark, bizdate)
        process_dws_layer(spark, bizdate)
        process_ads_layer(spark, bizdate)

        print("\nPipeline executed successfully!")

        # 验证结果
        print("Generated reports:")
        spark.sql(f"SELECT * FROM ads_discount_kpi WHERE dt='{bizdate}'").show(truncate=False)

    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()