from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sum, col, coalesce, round
from pyspark.sql import DataFrame


def get_spark_session():
    """初始化SparkSession，使用tms数据库"""
    spark = SparkSession.builder \
        .appName("TMSExpressProvinceStatsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark


def write_to_ads_express_province_stats(df: DataFrame):
    """写入目标表ads_express_province_stats，增加类型校验和错误处理"""
    try:
        # 1. 校验目标表结构
        target_table = "tms_ads.ads_express_province_stats"
        spark = get_spark_session()
        target_df = spark.table(target_table)

        # 2. 按目标表列顺序和类型调整DataFrame
        df = df.select([col(c).cast(target_df.schema[c].dataType) for c in target_df.columns])

        df.write \
            .mode("append") \
            .insertInto(target_table)

        print(f"[INFO] 成功写入 {df.count()} 条新数据到 {target_table}")

    except Exception as e:
        print(f"[ERROR] 写入失败：{str(e)}")
        print("[ERROR] 待写入数据样例：")
        df.show(5)
        raise e  # 抛出异常，终止作业


def execute_express_province_stats_etl(target_date: str):
    spark = get_spark_session()
    print(f"[INFO] 开始执行快递省份统计ETL，目标日期：{target_date}")

    # 1. 处理1天数据（1d表）
    # 1.1 派送成功数据
    province_deliver_1d = spark.table("tms_dws.dws_trans_org_deliver_suc_1d") \
        .filter(col("dt") == target_date) \
        .groupBy("province_id", "province_name") \
        .agg(sum("order_count").alias("deliver_suc_count")) \
        .withColumn("dt", lit(target_date)) \
        .withColumn("recent_days", lit(1))

    # 1.2 分拣数据
    province_sort_1d = spark.table("tms_dws.dws_trans_org_sort_1d") \
        .filter(col("dt") == target_date) \
        .groupBy("province_id", "province_name") \
        .agg(sum("sort_count").alias("sort_count")) \
        .withColumn("dt", lit(target_date)) \
        .withColumn("recent_days", lit(1))

    # 1.3 揽收数据（金额保留两位小数）
    province_receive_1d = spark.table("tms_dws.dws_trans_org_receive_1d") \
        .filter(col("dt") == target_date) \
        .groupBy("province_id", "province_name") \
        .agg(
        sum("order_count").alias("receive_order_count"),
        round(sum("order_amount"), 2).alias("receive_order_amount")
    ) \
        .withColumn("dt", lit(target_date)) \
        .withColumn("recent_days", lit(1))

    # 1.4 1d部分关联
    part1d = province_deliver_1d \
        .join(province_sort_1d, on=["dt", "recent_days", "province_id", "province_name"], how="full_outer") \
        .join(province_receive_1d, on=["dt", "recent_days", "province_id", "province_name"], how="full_outer") \
        .select(
        coalesce(province_deliver_1d["dt"], province_sort_1d["dt"], province_receive_1d["dt"]).alias("dt"),
        coalesce(province_deliver_1d["recent_days"], province_sort_1d["recent_days"], province_receive_1d["recent_days"]).alias(
            "recent_days"),
        coalesce(province_deliver_1d["province_id"], province_sort_1d["province_id"], province_receive_1d["province_id"]).alias("province_id"),
        coalesce(province_deliver_1d["province_name"], province_sort_1d["province_name"], province_receive_1d["province_name"]).alias(
            "province_name"),
        col("receive_order_count"),
        col("receive_order_amount"),
        col("deliver_suc_count"),
        col("sort_count")
    )

    # 2. 处理多天数据（nd表）
    # 2.1 派送成功数据
    province_deliver_nd = spark.table("tms_dws.dws_trans_org_deliver_suc_nd") \
        .filter(col("dt") == target_date) \
        .groupBy("recent_days", "province_id", "province_name") \
        .agg(sum("order_count").alias("deliver_suc_count")) \
        .withColumn("dt", lit(target_date))

    # 2.2 分拣数据
    province_sort_nd = spark.table("tms_dws.dws_trans_org_sort_nd") \
        .filter(col("dt") == target_date) \
        .groupBy("recent_days", "province_id", "province_name") \
        .agg(sum("sort_count").alias("sort_count")) \
        .withColumn("dt", lit(target_date))

    # 2.3 揽收数据
    province_receive_nd = spark.table("tms_dws.dws_trans_org_receive_nd") \
        .filter(col("dt") == target_date) \
        .groupBy("recent_days", "province_id", "province_name") \
        .agg(
        sum("order_count").alias("receive_order_count"),
        round(sum("order_amount"), 2).alias("receive_order_amount")
    ) \
        .withColumn("dt", lit(target_date))

    # 2.4 nd部分关联
    partnd = province_deliver_nd \
        .join(province_sort_nd, on=["dt", "recent_days", "province_id", "province_name"], how="full_outer") \
        .join(province_receive_nd, on=["dt", "recent_days", "province_id", "province_name"], how="full_outer") \
        .select(
        coalesce(province_deliver_nd["dt"], province_sort_nd["dt"], province_receive_nd["dt"]).alias("dt"),
        coalesce(province_deliver_nd["recent_days"], province_sort_nd["recent_days"], province_receive_nd["recent_days"]).alias(
            "recent_days"),
        coalesce(province_deliver_nd["province_id"], province_sort_nd["province_id"], province_receive_nd["province_id"]).alias("province_id"),
        coalesce(province_deliver_nd["province_name"], province_sort_nd["province_name"], province_receive_nd["province_name"]).alias(
            "province_name"),
        col("receive_order_count"),
        col("receive_order_amount"),
        col("deliver_suc_count"),
        col("sort_count")
    )

    # 3. 合并当前日期的新数据（不包含历史数据，避免重复写入）
    final_df = part1d.unionByName(partnd)

    # 4. 按目标表列顺序整理
    target_columns = [
        "dt", "recent_days", "province_id", "province_name",
        "receive_order_count", "receive_order_amount",
        "deliver_suc_count", "sort_count"
    ]
    final_df = final_df.select(target_columns)

    # 5. 写入目标表
    write_to_ads_express_province_stats(final_df)
    print(f"[INFO] ETL执行完成，目标日期：{target_date}")


if __name__ == "__main__":
    target_date = '20250713'  # 可通过调度工具动态传入（如Airflow/Oozie）
    execute_express_province_stats_etl(target_date)