from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, substring, date_format, from_utc_timestamp, to_timestamp, when

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("OrderPaymentETL") \
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
    df.write \
        .mode('append') \
        .insertInto(f"tms_dwd.{tableName}")

def process_order_payment(partition_date: str, tableName):
    """处理订单支付信息并写入Hive"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理订单支付信息，目标分区日期：{partition_date}")
    source_dt = "20250713"  # 源数据分区日期

    # 1. 处理支付信息(pay_info)
    # 1.1 基础订单信息
    without_status_df = spark.table("tms.ods_order_info") \
        .filter((col("dt") == source_dt) & (col("status") == "60020") & (col("is_deleted") == "0")) \
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
        date_format(
            from_utc_timestamp(
                to_timestamp(col("estimate_arrive_time")),
                "UTC"
            ),
            "yyyy-MM-dd HH:mm:ss"
        ).alias("estimate_arrive_time"),
        col("distance"),
        date_format(
            from_utc_timestamp(
                to_timestamp(
                    concat(
                        substring(col("update_time"), 1, 10),
                        lit(" "),
                        substring(col("update_time"), 12, 8)
                    ),
                    "yyyy-MM-dd HH:mm:ss"
                ),
                "GMT+8"
            ),
            "yyyy-MM-dd HH:mm:ss"
        ).alias("payment_time")
    )

    # 1.2 字典表关联
    dic_df = spark.table("tms.ods_base_dic") \
        .filter((col("dt") == source_dt) & (col("is_deleted") == "0")) \
        .select(col("id"), col("name"))

    # 1.3 支付信息最终DF
    pay_info_df = without_status_df \
        .join(dic_df.alias("dic_for_status"),
              without_status_df["status"] == col("dic_for_status.id").cast("string"),
              "left") \
        .join(dic_df.alias("dic_type_name"),
              without_status_df["payment_type"] == col("dic_type_name.id").cast("string"),
              "left") \
        .select(
        without_status_df["id"],
        col("order_no"),
        col("status"),
        col("dic_for_status.name").alias("status_name"),
        col("collect_type"),
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
        col("dic_type_name.name").alias("payment_type_name"),
        col("cargo_num"),
        col("amount"),
        col("estimate_arrive_time"),
        col("distance"),
        col("payment_time")
    )

    # 2. 处理订单信息(order_info)
    # 2.1 从dwd表获取已完成订单 - 修正列名拼写
    dwd_order_df = spark.table("tms_dwd.dwd_trade_order_detail_inc") \
        .filter((col("dt") == "9999-12-31") & (col("status") == "60010")) \
        .select(
        col("id"),
        col("order_id"),
        col("cargo_type"),
        col("cargo_type_name"),
        col("volumn_length"),  # 使用正确的列名
        col("volumn_width"),   # 使用正确的列名
        col("volumn_height"),  # 使用正确的列名
        col("weight"),
        lit(None).alias("payment_time"),
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
        lit(None).alias("payment_type"),
        lit(None).alias("payment_type_name"),
        col("cargo_num"),
        col("amount"),
        col("estimate_arrive_time"),
        col("distance")
    )

    # 2.2 从原始表获取新订单
    # 2.2.1 货物信息
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
                to_timestamp(
                    concat(
                        substring(col("create_time"), 1, 10),
                        lit(" "),
                        substring(col("create_time"), 12, 8)
                    ),
                    "yyyy-MM-dd HH:mm:ss"
                ),
                "GMT+8"
            ),
            "yyyy-MM-dd HH:mm:ss"
        ).alias("order_time")
    )

    # 2.2.2 订单信息
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
        col("estimate_arrive_time"),
        col("distance")
    )

    # 2.2.3 关联处理
    new_order_df = cargo_df \
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
        col("volume_length").alias("volumn_length"),  # 确保与目标表列名一致
        col("volume_width").alias("volumn_width"),    # 确保与目标表列名一致
        col("volume_height").alias("volumn_height"),  # 确保与目标表列名一致
        col("weight"),
        col("order_time").alias("payment_time"),
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
        lit(None).alias("payment_type"),
        lit(None).alias("payment_type_name"),
        col("cargo_num"),
        col("amount"),
        col("estimate_arrive_time"),
        col("distance")
    )

    # 2.3 合并订单信息
    order_info_df = dwd_order_df.union(new_order_df)

    # 3. 最终关联结果
    result_df = pay_info_df \
        .join(order_info_df, pay_info_df["id"] == order_info_df["order_id"], "inner") \
        .select(
        order_info_df["id"],
        col("order_id"),
        col("cargo_type"),
        col("cargo_type_name"),
        col("volumn_length"),
        col("volumn_width"),
        col("volumn_height"),
        col("weight"),
        pay_info_df["payment_time"],
        order_info_df["order_no"],
        pay_info_df["status"],
        pay_info_df["status_name"],
        order_info_df["collect_type"],
        col("collect_type_name"),
        order_info_df["user_id"],
        order_info_df["receiver_complex_id"],
        order_info_df["receiver_province_id"],
        order_info_df["receiver_city_id"],
        order_info_df["receiver_district_id"],
        order_info_df["receiver_name"],
        order_info_df["sender_complex_id"],
        order_info_df["sender_province_id"],
        order_info_df["sender_city_id"],
        order_info_df["sender_district_id"],
        order_info_df["sender_name"],
        pay_info_df["payment_type"],
        pay_info_df["payment_type_name"],
        order_info_df["cargo_num"],
        order_info_df["amount"],
        order_info_df["estimate_arrive_time"],
        order_info_df["distance"],
        lit(partition_date).alias("dt")  # 添加分区字段
    )

    print(f"[INFO] 数据处理完成，准备写入分区 {partition_date}")
    result_df.show(5)

    # 写入Hive
    select_to_hive(result_df, tableName, partition_date)

if __name__ == "__main__":
    target_table = 'dwd_trade_pay_suc_detail_inc'
    target_partition = '20250725'
    process_order_payment(target_partition, target_table)