from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, substring, date_format, from_utc_timestamp, to_timestamp

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("OrderCargoDetailETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark

def select_to_hive(df, tableName, partition_date):
    """将DataFrame写入Hive分区表"""
    # 移除partitionBy()，因为insertInto()会自动识别表的分区
    df.write \
        .mode('append') \
        .insertInto(f"tms_dwd.{tableName}")

def process_order_cargo_detail(partition_date: str, tableName):
    """处理订单货物详情并写入Hive"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理订单货物详情，目标分区日期：{partition_date}")
    source_dt = "20250713"  # 源数据分区日期

    # 1. 处理ods_order_cargo表
    cargo_df = spark.table("tms.ods_order_cargo") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id"),
        col("order_id"),
        col("cargo_type"),
        col("volume_length"),
        col("volume_width"),
        col("volume_height"),
        col("weight"),
        date_format(
            from_utc_timestamp(
                to_timestamp(col("create_time"), "yyyy-MM-dd HH:mm:ss"),
                "GMT+8"
            ),
            "yyyy-MM-dd HH:mm:ss"
        ).alias("order_time")
    )

    # 2. 处理ods_order_info表
    info_df = spark.table("tms.ods_order_info") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id"),
        col("order_no"),
        col("status"),
        col("collect_type"),
        col("user_id"),
        col("receiver_complex_id"),
        col("receiver_province_id"),
        col("receiver_city_id"),
        col("receiver_district_id"),
        concat(substring(col("receiver_name"), 1, 1), lit("*")).alias("receiver_name"),
        col("sender_complex_id"),
        col("sender_province_id"),
        col("sender_city_id"),
        col("sender_district_id"),
        concat(substring(col("sender_name"), 1, 1), lit("*")).alias("sender_name"),
        col("cargo_num"),
        col("amount"),
        date_format(
            from_utc_timestamp(
                to_timestamp(col("estimate_arrive_time")),
                "UTC"
            ),
            "yyyy-MM-dd HH:mm:ss"
        ).alias("estimate_arrive_time"),
        col("distance")
    )

    # 3. 处理字典表
    dic_df = spark.table("tms.ods_base_dic") \
        .filter((col("dt") == source_dt) & (col("is_deleted") == "0")) \
        .select(col("id"), col("name"))

    # 4. 执行多表关联
    result_df = cargo_df \
        .join(info_df, cargo_df["order_id"] == info_df["id"], "inner") \
        .join(dic_df.alias("dic_for_cargo_type"),
              cargo_df["cargo_type"] == col("dic_for_cargo_type.id").cast("string"),
              "left") \
        .join(dic_df.alias("dic_for_status"),
              info_df["status"] == col("dic_for_status.id").cast("string"),
              "left") \
        .join(dic_df.alias("dic_for_collect_type"),
              info_df["collect_type"] == col("dic_for_collect_type.id").cast("string"),
              "left") \
        .select(
        cargo_df["id"],
        col("order_id"),
        col("cargo_type"),
        col("dic_for_cargo_type.name").alias("cargo_type_name"),
        col("volume_length"),
        col("volume_width"),
        col("volume_height"),
        col("weight"),
        col("order_time"),
        col("order_no"),
        col("status"),
        col("dic_for_status.name").alias("status_name"),
        col("collect_type"),
        col("dic_for_collect_type.name").alias("collect_type_name"),
        col("user_id"),
        col("receiver_complex_id"),
        col("receiver_province_id"),
        col("receiver_city_id"),
        col("receiver_district_id"),
        col("receiver_name"),
        col("sender_complex_id"),
        col("sender_province_id"),
        col("sender_city_id"),
        col("sender_district_id"),
        col("sender_name"),
        col("cargo_num"),
        col("amount"),
        col("estimate_arrive_time"),
        col("distance"),
        lit(partition_date).alias("dt")  # 添加目标分区字段
    )

    print(f"[INFO] 数据处理完成，准备写入分区 {partition_date}")
    result_df.show(5)

    # 写入Hive
    select_to_hive(result_df, tableName, partition_date)

if __name__ == "__main__":
    target_table = 'dwd_trade_order_detail_inc'
    target_partition = '20250725'
    process_order_cargo_detail(target_partition, target_table)