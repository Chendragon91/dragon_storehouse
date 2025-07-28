from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, date_format, lit, concat, substr, cast

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("OrderCancelETL") \
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
    jdbcDF.write \
        .mode('append') \
        .insertInto(f"tms_dwd.{tableName}")

def process_order_cancel_info(partition_date: str, tableName):
    """处理订单取消信息并写入Hive"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理订单取消信息，目标分区日期：{partition_date}")
    source_dt = "20250713"  # 源数据分区日期

    # 1. 处理cancel_info部分
    # 1.1 处理without_status部分（已修复substr参数问题）
    without_status = spark.table("tms.ods_order_info") \
        .filter((col("dt") == source_dt) & (col("status") == "60999") & (col("is_deleted") == "0")) \
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
        date_format(from_unixtime(col("update_time").cast("bigint") / 1000), "yyyy-MM-dd HH:mm:ss").alias("cancel_time")
    )

    # 1.2 处理dic_for_status部分
    dic_for_status = spark.table("tms.ods_base_dic") \
        .filter((col("dt") == source_dt) & (col("is_deleted") == "0")) \
        .select(
        col("id"),
        col("name")
    )

    # 1.3 合并得到cancel_info（修复字段引用问题）
    cancel_info = without_status.join(
        dic_for_status,
        without_status.status == cast(dic_for_status["id"], "string"),
        "left"
    ).select(
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
        without_status["cargo_num"],
        without_status["amount"],
        without_status["estimate_arrive_time"],
        without_status["distance"],
        without_status["cancel_time"]
    )

    # 2. 处理order_info部分（统一字段命名）
    # 2.1 处理cargo部分
    cargo = spark.table("tms.ods_order_cargo") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id"),
        col("order_id"),
        col("cargo_type"),
        col("volume_length").alias("volumn_length"),  # 保持与SQL别名一致
        col("volume_width").alias("volumn_width"),
        col("volume_height").alias("volumn_height"),
        col("weight")
    )

    # 2.2 处理info部分（修复sender_province_id拼写错误）
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
        col("sender_province_id"),  # 修正拼写：proivnce -> province
        col("sender_city_id"),
        col("sender_district_id"),
        concat(substr(col("sender_name"), lit(1), lit(1)), lit("*")).alias("sender_name"),
        col("cargo_num"),
        col("amount"),
        date_format(from_unixtime(col("estimate_arrive_time")), "yyyy-MM-dd HH:mm:ss").alias("estimate_arrive_time"),
        col("distance")
    )

    # 2.3 处理字典表
    dic_for_cargo_type = spark.table("tms.ods_base_dic") \
        .filter((col("dt") == source_dt) & (col("is_deleted") == "0")) \
        .select(
        col("id"),
        col("name")
    )

    dic_for_status_order = dic_for_status  # 复用之前查询

    dic_for_collect_type = spark.table("tms.ods_base_dic") \
        .filter((col("dt") == source_dt) & (col("is_deleted") == "0")) \
        .select(
        col("id"),
        col("name")
    )

    # 2.4 合并order_info（添加缺失的字段）
    order_info = cargo.join(
        info,
        cargo.order_id == info.id,
        "inner"
    ).join(
        dic_for_cargo_type,
        cargo.cargo_type == cast(dic_for_cargo_type["id"], "string"),
        "left"
    ).join(
        dic_for_status_order,
        info.status == cast(dic_for_status_order["id"], "string"),
        "left"
    ).join(
        dic_for_collect_type,
        info.collect_type == cast(dic_for_collect_type["id"], "string"),
        "left"
    ).select(
        cargo["id"],
        cargo["order_id"],
        cargo["cargo_type"],
        dic_for_cargo_type["name"].alias("cargo_type_name"),
        cargo["volumn_length"],
        cargo["volumn_width"],
        cargo["volumn_height"],
        cargo["weight"],
        info["order_no"],
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
        info["cargo_num"],
        info["amount"],
        info["estimate_arrive_time"],
        info["distance"]
    )

    # 3. 最终合并（修正字段引用）
    final_df = cancel_info.join(
        order_info,
        cancel_info.id == order_info.order_id,
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
        cancel_info["cancel_time"],
        order_info["order_no"],
        cancel_info["status"],
        cancel_info["status_name"],
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
        order_info["cargo_num"],
        order_info["amount"],
        order_info["estimate_arrive_time"],
        order_info["distance"]
    ).withColumn("ds", lit(partition_date))

    print(f"[INFO] 数据样例：")
    final_df.show(5, truncate=False)

    # 写入Hive
    select_to_hive(final_df, tableName, partition_date)
    print(f"[INFO] 数据成功写入 {tableName} 表 {partition_date} 分区")

if __name__ == "__main__":
    target_table = 'dwd_trade_order_cancel_detail_inc'  # 与报错中的文件名保持一致
    target_partition = '20250725'
    process_order_cancel_info(target_partition, target_table)