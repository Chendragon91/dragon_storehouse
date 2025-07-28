from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, date_format, lit, concat, substr, cast

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("BoundFinishETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark

def select_to_hive(df, table_name, partition_date):
    """将DataFrame写入Hive分区表"""
    df.withColumn("dt", lit(partition_date)) \
        .write \
        .mode('append') \
        .insertInto(f"tms_dwd.{table_name}")

def process_bound_finish_info(partition_date: str, table_name):
    """处理绑定完成信息并写入Hive"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理绑定完成信息，目标分区日期：{partition_date}")
    source_dt = "20250713"

    # 1. 处理bound_finish_info部分
    # 1.1 without_status部分
    without_status = spark.table("tms.ods_order_info") \
        .filter((col("dt") == source_dt) & (col("status") == "60060") & (col("is_deleted") == "0")) \
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
        concat(substr(col("receiver_name"), lit(1), lit(1)), lit("*")).alias("receiver_name"),
        col("sender_complex_id"),
        col("sender_province_id"),
        col("sender_city_id"),
        col("sender_district_id"),
        concat(substr(col("sender_name"), lit(1), lit(1)), lit("*")).alias("sender_name"),
        col("payment_type"),
        col("cargo_num"),
        col("amount"),
        date_format(from_unixtime(col("estimate_arrive_time")), "yyyy-MM-dd HH:mm:ss").alias("estimate_arrive_time"),
        col("distance"),
        date_format(from_unixtime(col("update_time").cast("bigint") / 1000), "yyyy-MM-dd HH:mm:ss").alias("bound_finish_time")
    )

    # 1.2 字典表查询
    dic_for_status = spark.table("tms.ods_base_dic") \
        .filter((col("dt") == source_dt) & (col("is_deleted") == "0")) \
        .select(col("id"), col("name"))

    dic_type_name = spark.table("tms.ods_base_dic") \
        .filter((col("dt") == source_dt) & (col("is_deleted") == "0")) \
        .select(col("id"), col("name"))

    # 1.3 合并bound_finish_info
    bound_finish_info = without_status \
        .join(dic_for_status, without_status.status == cast(dic_for_status["id"], "string"), "left") \
        .join(dic_type_name, without_status.payment_type == cast(dic_type_name["id"], "string"), "left") \
        .select(
        without_status["id"],
        without_status["order_no"],
        without_status["status"],
        dic_for_status["name"].alias("status_name"),
        without_status["collect_type"],
        without_status["user_id"],
        without_status["receiver_complex_id"],
        without_status["receiver_province_id"],
        without_status["receiver_city_id"],
        without_status["receiver_district_id"],
        without_status["receiver_name"],
        without_status["sender_complex_id"],
        without_status["sender_province_id"],
        without_status["sender_city_id"],
        without_status["sender_district_id"],
        without_status["sender_name"],
        without_status["payment_type"],
        dic_type_name["name"].alias("payment_type_name"),
        without_status["cargo_num"],
        without_status["amount"],
        without_status["estimate_arrive_time"],
        without_status["distance"],
        without_status["bound_finish_time"]
    )

    # 2. 处理order_info部分
    # 2.1 第一部分：从dwd表查询
    dwd_part = spark.table("tms_dwd.dwd_trans_bound_finish_detail_inc") \
        .filter((col("dt") == "9999-12-31") &
                (col("status").isin(["60010", "60020", "60030", "60040", "60050"]))) \
        .select(
        col("id"),
        col("order_id"),
        col("cargo_type"),
        col("cargo_type_name"),
        col("volumn_length"),
        col("volumn_width"),
        col("volumn_height"),
        col("weight"),
        col("order_no"),
        col("status"),
        col("status_name"),
        col("collect_type"),
        col("collect_type_name"),
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
        col("payment_type"),
        col("payment_type_name"),
        col("cargo_num"),
        col("amount"),
        col("estimate_arrive_time"),
        col("distance")
    )

    # 2.2 第二部分：从ods表join
    # 2.2.1 cargo部分
    cargo = spark.table("tms.ods_order_cargo") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id"),
        col("order_id"),
        col("cargo_type"),
        col("volume_length"),
        col("volume_width"),
        col("volume_height"),
        col("weight"),
        date_format(from_unixtime(col("create_time").cast("bigint") / 1000), "yyyy-MM-dd HH:mm:ss").alias("order_time")
    )

    # 2.2.2 info部分
    info = spark.table("tms.ods_order_info") \
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
        concat(substr(col("receiver_name"), lit(1), lit(1)), lit("*")).alias("receiver_name"),
        col("sender_complex_id"),
        col("sender_province_id"),
        col("sender_city_id"),
        col("sender_district_id"),
        concat(substr(col("sender_name"), lit(1), lit(1)), lit("*")).alias("sender_name"),
        col("payment_type"),
        col("cargo_num"),
        col("amount"),
        date_format(from_unixtime(col("estimate_arrive_time")), "yyyy-MM-dd HH:mm:ss").alias("estimate_arrive_time"),
        col("distance")
    )

    # 2.2.3 字典表
    dic_for_cargo_type = spark.table("tms.ods_base_dic") \
        .filter((col("dt") == source_dt) & (col("is_deleted") == "0")) \
        .select(col("id"), col("name"))

    dic_for_status_order = dic_for_status  # 复用之前查询
    dic_for_collect_type = spark.table("tms.ods_base_dic") \
        .filter((col("dt") == source_dt) & (col("is_deleted") == "0")) \
        .select(col("id"), col("name"))

    # 2.2.4 合并第二部分
    ods_part = cargo.join(
        info, cargo.order_id == info.id, "inner"
    ).join(
        dic_for_cargo_type, cargo.cargo_type == cast(dic_for_cargo_type["id"], "string"), "left"
    ).join(
        dic_for_status_order, info.status == cast(dic_for_status_order["id"], "string"), "left"
    ).join(
        dic_for_collect_type, info.collect_type == cast(dic_for_collect_type["id"], "string"), "left"
    ).select(
        cargo["id"],
        cargo["order_id"],
        cargo["cargo_type"],
        dic_for_cargo_type["name"].alias("cargo_type_name"),
        cargo["volume_length"].alias("volumn_length"),
        cargo["volume_width"].alias("volumn_width"),
        cargo["volume_height"].alias("volumn_height"),
        cargo["weight"],
        cargo["order_time"].alias("order_no"),
        info["status"],
        dic_for_status_order["name"].alias("status_name"),
        info["collect_type"],
        dic_for_collect_type["name"].alias("collect_type_name"),
        info["user_id"],
        info["receiver_complex_id"],
        info["receiver_province_id"],
        info["receiver_city_id"],
        info["receiver_district_id"],
        info["receiver_name"],
        info["sender_complex_id"],
        info["sender_province_id"],
        info["sender_city_id"],
        info["sender_district_id"],
        info["sender_name"],
        info["payment_type"],
        lit("").alias("payment_type_name"),
        info["cargo_num"],
        info["amount"],
        info["estimate_arrive_time"],
        info["distance"]
    )

    # 2.3 合并两部分order_info
    order_info = dwd_part.unionAll(ods_part)

    # 3. 最终合并
    final_df = bound_finish_info.join(
        order_info,
        bound_finish_info.id == order_info.order_id,
        "inner"
    ).select(
        order_info["id"],
        order_info["order_id"],
        order_info["cargo_type"],
        order_info["cargo_type_name"],
        order_info["volumn_length"],
        order_info["volumn_width"],
        order_info["volumn_height"],
        order_info["weight"],
        bound_finish_info["bound_finish_time"],
        order_info["order_no"],
        bound_finish_info["status"],
        bound_finish_info["status_name"],
        order_info["collect_type"],
        order_info["collect_type_name"],
        order_info["user_id"],
        order_info["receiver_complex_id"],
        order_info["receiver_province_id"],
        order_info["receiver_city_id"],
        order_info["receiver_district_id"],
        order_info["receiver_name"],
        order_info["sender_complex_id"],
        order_info["sender_province_id"],
        order_info["sender_city_id"],
        order_info["sender_district_id"],
        order_info["sender_name"],
        bound_finish_info["payment_type"],
        bound_finish_info["payment_type_name"],
        order_info["cargo_num"],
        order_info["amount"],
        order_info["estimate_arrive_time"],
        order_info["distance"]
    )

    print("[INFO] 数据样例：")
    final_df.show(5, truncate=False)

    # 写入Hive
    select_to_hive(final_df, table_name, partition_date)
    print(f"[INFO] 数据成功写入 {table_name} 表 {partition_date} 分区")

if __name__ == "__main__":
    target_table = 'dwd_trans_bound_finish_detail_inc'  # 目标表名
    target_partition = '20250725'  # 目标分区
    process_bound_finish_info(target_partition, target_table)