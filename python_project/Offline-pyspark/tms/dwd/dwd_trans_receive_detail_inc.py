from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, date_format, lit, concat, substring, cast

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("OrderReceiveETL") \
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
    jdbcDF.drop("dt").write \
        .mode('append') \
        .insertInto(f"tms_dwd.{tableName}")

def process_receive_info(partition_date: str, tableName):
    """处理订单接收信息并写入Hive"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理订单接收信息，目标分区日期：{partition_date}")
    source_dt = "20250713"  # 源数据分区日期

    # 读取源表数据
    ods_order_info = spark.table("tms.ods_order_info").filter(col("dt") == source_dt).alias("order_info")
    ods_base_dic = spark.table("tms.ods_base_dic").filter((col("dt") == source_dt) & (col("is_deleted") == "0")).alias("base_dic")
    ods_order_cargo = spark.table("tms.ods_order_cargo").filter(col("dt") == source_dt).alias("order_cargo")
    dwd_trade_order_cancel_detail_inc = spark.table("tms_dwd.dwd_trade_order_cancel_detail_inc").filter(col("dt") == "9999-12-31").alias("cancel_detail")

    # 构建receive_info
    without_status = ods_order_info.filter((col("status") == "60030") & (col("is_deleted") == "0")) \
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
        date_format(from_unixtime(col("update_time")), "yyyy-MM-dd HH:mm:ss").alias("receive_time")
    ).alias("without_status")

    dic_for_status = ods_base_dic.select(col("id"), col("name")).alias("status_dic")
    dic_type_name = ods_base_dic.select(col("id"), col("name")).alias("type_dic")

    receive_info = without_status \
        .join(dic_for_status, col("without_status.status") == cast(col("status_dic.id"), "string"), "left") \
        .join(dic_type_name, col("without_status.payment_type") == cast(col("type_dic.id"), "string"), "left") \
        .select(
        col("without_status.id"),
        col("without_status.order_no"),
        col("without_status.status"),
        col("status_dic.name").alias("status_name"),
        col("without_status.collect_type"),
        col("without_status.user_id"),
        col("without_status.receiver_complex_id"),
        col("without_status.receiver_province_id"),
        col("without_status.receiver_city_id"),
        col("without_status.receiver_district_id"),
        col("without_status.receiver_name"),
        col("without_status.sender_complex_id"),
        col("without_status.sender_province_id"),
        col("without_status.sender_city_id"),
        col("without_status.sender_district_id"),
        col("without_status.sender_name"),
        col("without_status.payment_type"),
        col("type_dic.name").alias("payment_type_name"),
        col("without_status.cargo_num"),
        col("without_status.amount"),
        col("without_status.estimate_arrive_time"),
        col("without_status.distance"),
        col("without_status.receive_time")
    ).alias("receive_info")

    # 构建order_info第一部分 (canceled orders)
    order_info_part1 = dwd_trade_order_cancel_detail_inc.filter(
        (col("status") == "60010") | (col("status") == "60020")
    ).select(
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
        lit("").alias("payment_type"),
        lit("").alias("payment_type_name"),
        col("cargo_num"),
        col("amount"),
        col("estimate_arrive_time"),
        col("distance")
    ).alias("order_info_part1")

    # 构建order_info第二部分 (cargo info)
    cargo = ods_order_cargo.select(
        col("id"),
        col("order_id"),
        col("cargo_type"),
        col("volume_length").alias("volumn_length"),
        col("volume_width").alias("volumn_width"),
        col("volume_height").alias("volumn_height"),
        col("weight")
    ).alias("cargo")

    info = ods_order_info.select(
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
        date_format(from_unixtime(col("estimate_arrive_time")), "yyyy-MM-dd HH:mm:ss").alias("estimate_arrive_time"),
        col("distance")
    ).alias("info")

    dic_for_cargo_type = ods_base_dic.select(col("id"), col("name")).alias("cargo_type_dic")
    dic_for_status = ods_base_dic.select(col("id"), col("name")).alias("status_dic2")
    dic_for_collect_type = ods_base_dic.select(col("id"), col("name")).alias("collect_type_dic")

    order_info_part2 = cargo.join(info, col("cargo.order_id") == col("info.id")) \
        .join(dic_for_cargo_type, col("cargo.cargo_type") == cast(col("cargo_type_dic.id"), "string"), "left") \
        .join(dic_for_status, col("info.status") == cast(col("status_dic2.id"), "string"), "left") \
        .join(dic_for_collect_type, col("info.collect_type") == cast(col("collect_type_dic.id"), "string"), "left") \
        .select(
        col("cargo.id"),
        col("cargo.order_id"),
        col("cargo.cargo_type"),
        col("cargo_type_dic.name").alias("cargo_type_name"),
        col("cargo.volumn_length"),
        col("cargo.volumn_width"),
        col("cargo.volumn_height"),
        col("cargo.weight"),
        col("info.order_no"),
        col("info.status"),
        col("status_dic2.name").alias("status_name"),
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
        lit("").alias("payment_type"),
        lit("").alias("payment_type_name"),
        col("info.cargo_num"),
        col("info.amount"),
        col("info.estimate_arrive_time"),
        col("info.distance")
    ).alias("order_info_part2")

    # 合并order_info
    order_info = order_info_part1.union(order_info_part2).alias("order_info")

    # 最终结果
    final_df = receive_info.join(order_info, col("receive_info.id") == col("order_info.order_id")) \
        .select(
        col("order_info.id"),
        col("order_info.order_id"),
        col("order_info.cargo_type"),
        col("order_info.cargo_type_name"),
        col("order_info.volumn_length"),
        col("order_info.volumn_width"),
        col("order_info.volumn_height"),
        col("order_info.weight"),
        col("receive_info.receive_time"),
        col("order_info.order_no"),
        col("receive_info.status"),
        col("receive_info.status_name"),
        col("order_info.collect_type"),
        col("order_info.collect_type_name"),
        col("order_info.user_id"),
        col("order_info.receiver_complex_id"),
        col("order_info.receiver_province_id"),
        col("order_info.receiver_city_id"),
        col("order_info.receiver_district_id"),
        col("order_info.receiver_name"),
        col("order_info.sender_complex_id"),
        col("order_info.sender_province_id"),
        col("order_info.sender_city_id"),
        col("order_info.sender_district_id"),
        col("order_info.sender_name"),
        col("receive_info.payment_type"),
        col("receive_info.payment_type_name"),
        col("order_info.cargo_num"),
        col("order_info.amount"),
        col("order_info.estimate_arrive_time"),
        col("order_info.distance")
    )

    # 添加目标分区字段
    final_df = final_df.withColumn("ds", lit(partition_date))

    print(f"[INFO] 数据处理完成，准备写入分区{partition_date}")
    final_df.show(5)  # 预览5条数据

    # 写入Hive
    select_to_hive(final_df, tableName, partition_date)

if __name__ == "__main__":
    target_table = 'dwd_trans_receive_detail_inc'  # 目标表名
    target_partition = '20250725'  # 目标分区字段值
    process_receive_info(target_partition, target_table)