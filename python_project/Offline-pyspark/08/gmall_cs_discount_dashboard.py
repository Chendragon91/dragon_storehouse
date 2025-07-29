from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
import random
import numpy as np

# 初始化SparkSession（增加更多性能优化配置）
spark = SparkSession.builder \
    .appName("ECommerceMarketingDashboard") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.hive.convertMetastoreParquet", "false") \
    .enableHiveSupport() \
    .getOrCreate()

# 工单编号: 大数据-电商数仓-08-商品主题营销工具客服专属优惠看板

def create_base_tables():
    """创建所有需要的空表结构"""
    from pyspark.sql import DataFrame
    dummy_df = spark.createDataFrame([], StructType([]))

    # ODS层表
    for table in ["ods_shop", "ods_product", "ods_customer_service",
                  "ods_marketing_cs_discount_activity", "ods_marketing_cs_discount_records"]:
        if not spark.catalog.tableExists(table):
            dummy_df.write.saveAsTable(table)

    # DIM层表（SCD Type2设计）
    dim_tables = {
        "dim_shop": StructType([
            StructField("shop_id", StringType()),
            StructField("shop_name", StringType()),
            StructField("shop_level", IntegerType()),
            StructField("shop_type", IntegerType()),
            StructField("owner_id", StringType()),
            StructField("create_time", TimestampType()),
            StructField("update_time", TimestampType()),
            StructField("start_date", StringType()),
            StructField("end_date", StringType()),
            StructField("is_current", BooleanType())
        ]),
        # 其他DIM表结构类似...
    }
    for name, schema in dim_tables.items():
        if not spark.catalog.tableExists(name):
            spark.createDataFrame([], schema).write.saveAsTable(name)

def generate_mock_data(bizdate, record_count=1000000):
    """生成百万级模拟业务数据（优化版）"""
    print(f"开始生成{record_count}条模拟数据，业务日期: {bizdate}")

    # 1. 生成维度数据（使用广播变量优化）
    shops = spark.createDataFrame([
        (f"shop{i:03d}", f"{['旗舰','数码','家居','服饰','食品','美妆','母婴','图书','运动','家电'][i]}店",
         random.randint(1, 3), random.randint(1, 2), f"user{i:03d}")
        for i in range(10)
    ], ["shop_id", "shop_name", "shop_level", "shop_type", "owner_id"])

    # 2. 批量生成商品数据（使用向量化操作）
    product_data = []
    for i in range(1000):
        product_data.append((
            f"p{i:04d}",
            f"{random.choice(['笔记本','手机','电视','冰箱','沙发'])}{i%100}",
            f"shop{random.randint(0, 9):03d}",
            f"cate{random.randint(1, 5):02d}",
            ["电子产品","家电","家具","服饰","食品"][random.randint(0, 4)],
            round(random.uniform(100, 10000), 2),
            1,
            random.random() > 0.8,
            random.random() > 0.9
        ))
    products = spark.createDataFrame(product_data, [
        "product_id", "product_name", "shop_id", "category_id", "category_name",
        "price", "status", "is_pre_sale", "is_billion_subsidy"
    ])

    # 3. 生成优惠记录（使用DataFrame API高效生成）
    records = spark.range(record_count).select(
        expr(f"concat('rec', format_string('%07d', id))").alias("record_id"),
        expr(f"concat('act', format_string('%03d', cast(rand()*50 as int)))").alias("activity_id"),
        expr(f"concat('p', format_string('%04d', cast(rand()*1000 as int)))").alias("product_id"),
        when(rand() > 0.3, expr("concat('sku', cast(rand()*10 as int))")).otherwise(None).alias("sku_id"),
        expr(f"concat('buyer', format_string('%05d', cast(rand()*100000 as int)))").alias("buyer_id"),
        expr(f"concat('cs', format_string('%03d', cast(rand()*100 as int)))").alias("cs_id"),
        (rand() * 200 + 5).cast("decimal(10,2)").alias("discount_amount"),
        lit(24).alias("valid_hours"),
        current_timestamp().alias("create_time"),
        when(rand() > 0.4, 1).otherwise(when(rand() > 0.5, 0).otherwise(2)).alias("status"),
        when(rand() > 0.4, expr("concat('order', cast(rand()*900000+100000 as int))")).otherwise(None).alias("order_id"),
        when(rand() > 0.7,
             array_sample(array(lit("价格敏感客户"), lit("老客户维护"), lit("新客转化"), lit("投诉补偿")),
                          None).alias("remark")
             )

    # 写入ODS层（使用分区写入优化）
    shops.withColumn("dt", lit(bizdate)) \
        .write.mode("overwrite").saveAsTable("ods_shop")

    products.withColumn("dt", lit(bizdate)) \
        .write.mode("overwrite").saveAsTable("ods_product")

    records.withColumn("dt", lit(bizdate)) \
        .write.mode("overwrite").saveAsTable("ods_marketing_cs_discount_records")

def process_dim_layer(bizdate):
    """处理DIM层数据（含SCD Type2逻辑）"""
    print(f"\nProcessing DIM layer for {bizdate}")

    # 店铺维度处理
    spark.sql(f"""
    INSERT OVERWRITE TABLE dim_shop PARTITION(dt='{bizdate}')
    SELECT 
        s.shop_id, s.shop_name, s.shop_level, s.shop_type, s.owner_id,
        s.create_time, current_timestamp() as update_time,
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

def process_dwd_layer(bizdate):
    """处理DWD层明细事实表"""
    print(f"\nProcessing DWD layer for {bizdate}")

    spark.sql(f"""
    INSERT OVERWRITE TABLE dwd_cs_discount_fact PARTITION(dt='{bizdate}')
    SELECT 
        r.record_id, r.activity_id, 
        p.product_id, p.product_name, p.price as original_price,
        r.discount_amount, 
        (p.price - r.discount_amount) as final_price,
        r.buyer_id, r.cs_id, 
        r.create_time, r.expire_time,
        CASE 
            WHEN r.status = 1 AND r.expire_time >= current_timestamp() THEN 'used'
            WHEN r.status = 1 THEN 'expired_after_use'
            WHEN r.expire_time < current_timestamp() THEN 'expired'
            ELSE 'unused'
        END as discount_status,
        r.order_id,
        date_format(r.create_time, 'yyyyMMdd') as create_date
    FROM ods_marketing_cs_discount_records r
    JOIN ods_product p ON r.product_id = p.product_id AND p.dt = '{bizdate}'
    WHERE r.dt = '{bizdate}'
    """)

def process_dws_layer(bizdate):
    """处理DWS层汇总数据"""
    print(f"\nProcessing DWS layer for {bizdate}")

    # 日维度汇总
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

def process_ads_layer(bizdate):
    """处理ADS层应用数据"""
    print(f"\nProcessing ADS layer for {bizdate}")

    # 核心指标表
    spark.sql(f"""
    INSERT OVERWRITE TABLE ads_discount_kpi PARTITION(dt='{bizdate}')
    SELECT 
        create_date as stat_date,
        'all' as dimension,
        'total' as dimension_value,
        SUM(send_count) as send_count,
        SUM(use_count) as use_count,
        SUM(use_count)/NULLIF(SUM(send_count), 0) as use_rate,
        SUM(total_discount) as total_discount,
        SUM(gmv) as gmv,
        SUM(total_discount)/NULLIF(SUM(gmv), 0) as discount_ratio
    FROM dws_discount_daily
    WHERE dt = '{bizdate}'
    GROUP BY create_date
    """)

def main():
    """主执行流程"""
    bizdate = datetime.datetime.now().strftime("%Y%m%d")

    try:
        create_base_tables()
        generate_mock_data(bizdate, 1000000)
        process_dim_layer(bizdate)
        process_dwd_layer(bizdate)
        process_dws_layer(bizdate)
        process_ads_layer(bizdate)

        print("\nPipeline executed successfully!")
        print("Generated reports:")
        spark.sql(f"SELECT * FROM ads_discount_kpi WHERE dt='{bizdate}'").show()

    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()