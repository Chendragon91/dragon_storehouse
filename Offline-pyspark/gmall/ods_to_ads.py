from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


def create_spark_session():
    try:

        spark = SparkSession.builder \
            .appName('HiveDateWarehouseWithPySpark') \
            .config('hive.metastore.uris', 'thrift://cdh01:9083') \
            .config('spark.sql.warehouse.dir', '/warehouse/gmall') \
            .config('spark.driver.extraClassPath', '/path/to/hive/lib/*') \
            .config('spark.executor.extraClassPath', '/path/to/hive/lib/*') \
            .enableHiveSupport() \
            .getOrCreate()

        spark.sparkContext.setLogLevel('WARN')
        return spark
    except Exception as e:
        print(f"Failed to create SparkSession: {e}")
        return None


def load_ods_tables(spark, dt):
    tables = [
        "activity_info", "activity_info_full", "activity_rule", "activity_sku",
        "base_attr_info", "base_attr_value", "base_category1", "base_category2",
        "base_category3", "base_category_view", "base_dic", "base_frontend_param",
        "base_province", "base_region", "base_sale_attr", "base_trademark",
        "cart_info", "cms_banner", "comment_info", "coupon_info",
        "coupon_range", "coupon_use", "favor_info", "financial_sku_cost",
        "log_inc", "order_detail", "order_detail_activity", "order_detail_coupon",
        "order_info", "order_refund_info", "order_status_log", "payment_info",
        "promotion_pos", "promotion_refer", "refund_payment", "seckill_goods",
        "sku_attr_value", "sku_image", "sku_info", "sku_sale_attr_value",
        "spu_image", "spu_info", "spu_poster", "spu_sale_attr",
        "spu_sale_attr_value", "user_address", "user_info", "ware_info",
        "ware_order_task", "ware_order_task_detail", "ware_sku", "z_log"
    ]
    ods_tables = {}
    for table in tables:
        try:
            ods_tables[f"ods_{table}"] = spark.table(f"gmall.ods_{table}").filter(col("dt") == dt)
        except Exception as e:
            print(f"Failed to load table gmall.ods_{table}: {e}")
    return ods_tables


def create_dim_sku_full(ods_tables):
    try:
        dim_sku_full_df = ods_tables["ods_sku_info"].alias("sku") \
            .join(ods_tables["ods_spu_info"].alias("spu"), col("sku.spu_id") == col("spu.id"), "left") \
            .join(ods_tables["ods_base_category3"].alias("c3"), col("spu.category3_id") == col("c3.id"), "left") \
            .join(ods_tables["ods_base_category2"].alias("c2"), col("c3.category2_id") == col("c2.id"), "left") \
            .join(ods_tables["ods_base_category1"].alias("c1"), col("c2.category1_id") == col("c1.id"), "left") \
            .join(ods_tables["ods_base_trademark"].alias("tm"), col("tm.id") == col("sku.tm_id"), "left") \
            .join(ods_tables["ods_sku_attr_value"].alias("attr"), col("attr.sku_id") == col("sku.id"), "left") \
            .join(ods_tables["ods_sku_sale_attr_value"].alias("sale_attr"), col("sale_attr.sku_id") == col("sku.id"),
                  "left") \
            .select(
            col("sku.id").alias("sku_id"),
            col("sku.price"),
            col("sku.sku_name"),
            col("sku.sku_desc"),
            col("sku.weight"),
            col("sku.is_sale"),
            col("spu.id").alias("spu_id"),
            col("spu.spu_name"),
            col("c3.id").alias("category3_id"),
            col("c3.name").alias("category3_name"),
            col("c2.id").alias("category2_id"),
            col("c2.name").alias("category2_name"),
            col("c1.id").alias("category1_id"),
            col("c1.name").alias("category1_name"),
            col("tm.id").alias("tm_id"),
            col("tm.tm_name"),
            col("attr.id").alias("attr_id"),
            col("attr.value_id"),
            col("attr.attr_name"),
            col("attr.value_name"),
            col("sale_attr.id").alias("sale_attr_id"),
            col("sale_attr.sale_attr_value_id"),
            col("sale_attr.sale_attr_name"),
            col("sale_attr.sale_attr_value_name"),
            col("sku.create_time"),
            col("sku.dt")
        )

        dim_sku_full_df.write \
            .format("parquet") \
            .partitionBy("dt") \
            .mode("overwrite") \
            .saveAsTable("gmall.dim_sku_full")
    except Exception as e:
        print(f"Failed to create dim_sku_full: {e}")


# 创建商品维度表
def create_dim_coupon_full(ods_tables):
    try:
        # 从ODS层获取商品信息，处理维度变化
        dim_coupon_full = ods_tables["ods_coupon_info"].alias('ci') \
            .join(ods_tables["ods_base_dic"].alias('coupon_dic'), col('ci.coupon_type') == col('coupon_dic.dic_code'),
                  how='left') \
            .join(ods_tables["ods_base_dic"].alias('range_dic'), col('ci.range_type') == col('range_dic.dic_code'),
                  how='left') \
            .select(
            col('ci.id').alias('coupon_id'),
            col('ci.coupon_name'),
            col('ci.coupon_type'),
            col('coupon_dic.dic_name').alias('coupon_type_name'),
            col('ci.condition_amount'),
            col('ci.condition_num'),
            col('ci.activity_id'),
            col('ci.benefit_amount'),
            col('ci.benefit_discount')
            # benefit_rule=
        )

        # 写入Hive DIM层
        dim_coupon_full.write \
            .format("parquet") \
            .mode("overwrite") \
            .saveAsTable("dim.dim_item")

        print("商品维度表构建完成")
    except Exception as e:
        print(f"Failed to create dim_coupon_full: {e}")


if __name__ == "__main__":
    dt = "20250717"
    spark = create_spark_session()
    if spark:
        ods_tables = load_ods_tables(spark, dt)
        create_dim_sku_full(ods_tables)
        # create_dim_coupon_full(ods_tables)