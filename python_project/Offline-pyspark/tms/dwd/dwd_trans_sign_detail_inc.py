from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, date_format, lit, concat, substring, cast

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("OrderSignETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark

def select_to_hive(jdbcDF, tableName, partition_date):
    """将DataFrame写入Hive分区表"""
    jdbcDF.drop("ds").write \
        .mode('append') \
        .insertInto(f"tms_dwd.{tableName}")

def process_order_sign(partition_date: str, tableName):
    """处理订单签收信息并写入Hive"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理订单签收信息，目标分区日期：{partition_date}")
    source_dt = "20250713"  # 源数据分区日期

    # 读取源表数据
    ods_order_cargo = spark.table("tms.ods_order_cargo") \
        .filter((col("dt") == source_dt) & (col("is_deleted") == "0")) \
        .select(
        col("id"),
        col("order_id"),
        col("cargo_type"),
        col("volume_length"),
        col("volume_width"),
        col("volume_height"),
        col("weight")
    ).alias("cargo")

    ods_order_info = spark.table("tms.ods_order_info") \
        .filter((col("dt") == source_dt) & (col("is_deleted") == "0")) \
        .filter(~col("status").isin(["60010", "60020", "60030", "60040", "60050", "60060", "60070", "60999"])) \
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
        col("payment_type"),
        col("cargo_num"),
        col("amount"),
        date_format(from_unixtime(col("estimate_arrive_time")), "yyyy-MM-dd HH:mm:ss").alias("estimate_arrive_time"),
        col("distance"),
        concat(substring(col("update_time"), 1, 10), lit(" "), substring(col("update_time"), 12, 8)).alias("sign_time")
    ).alias("info")

    ods_base_dic = spark.table("tms.ods_base_dic") \
        .filter((col("dt") == source_dt) & (col("is_deleted") == "0")) \
        .select(col("id"), col("name"))

    # 创建字典表别名
    dic_for_cargo_type = ods_base_dic.alias("cargo_type_dic")
    dic_for_status = ods_base_dic.alias("status_dic")
    dic_for_collect_type = ods_base_dic.alias("collect_type_dic")
    dic_for_payment_type = ods_base_dic.alias("payment_type_dic")

    # 构建主查询
    result_df = ods_order_cargo.join(
        ods_order_info,
        col("cargo.order_id") == col("info.id"),
        "inner"
    ).join(
        dic_for_cargo_type,
        col("cargo.cargo_type") == cast(col("cargo_type_dic.id"), "string"),
        "left"
    ).join(
        dic_for_status,
        col("info.status") == cast(col("status_dic.id"), "string"),
        "left"
    ).join(
        dic_for_collect_type,
        col("info.collect_type") == cast(col("collect_type_dic.id"), "string"),
        "left"
    ).join(
        dic_for_payment_type,
        col("info.payment_type") == cast(col("payment_type_dic.id"), "string"),
        "left"
    ).select(
        col("cargo.id"),
        col("cargo.order_id"),
        col("cargo.cargo_type"),
        col("cargo_type_dic.name").alias("cargo_type_name"),
        col("cargo.volume_length"),
        col("cargo.volume_width"),
        col("cargo.volume_height"),
        col("cargo.weight"),
        col("info.sign_time"),
        col("info.order_no"),
        col("info.status"),
        col("status_dic.name").alias("status_name"),
        col("info.collect_type"),
        col("collect_type_dic.name").alias("collect_type_name"),
        col("info.user_id"),
        col("info.receiver_complex_id"),
        col("info.receiver_province_id"),
        col("info.receiver_city_id"),
        col("info.receiver_district_id"),
        col("info.receiver_name"),
        col("info.sender_complex_id"),
        col("info.sender_province_id"),
        col("info.sender_city_id"),
        col("info.sender_district_id"),
        col("info.sender_name"),
        col("info.payment_type"),
        col("payment_type_dic.name").alias("payment_type_name"),
        col("info.cargo_num"),
        col("info.amount"),
        col("info.estimate_arrive_time"),
        col("info.distance"),
        date_format(col("info.sign_time"), "yyyy-MM-dd").alias("dt")
    )

    # 添加目标分区字段
    final_df = result_df.withColumn("ds", lit(partition_date))

    print(f"[INFO] 数据处理完成，准备写入分区{partition_date}")
    final_df.show(5)  # 预览5条数据

    # 写入Hive
    select_to_hive(final_df, tableName, partition_date)

if __name__ == "__main__":
    target_table = 'dwd_trans_sign_detail_inc'  # 目标表名
    target_partition = '20250725'  # 目标分区字段值
    process_order_sign(target_partition, target_table)