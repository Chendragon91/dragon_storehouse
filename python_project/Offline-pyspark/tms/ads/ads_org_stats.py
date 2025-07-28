from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sum, col, coalesce, round, when
from pyspark.sql import DataFrame


def get_spark_session():
    """初始化SparkSession，使用tms数据库"""
    spark = SparkSession.builder \
        .appName("TMSOrgStatsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark


def write_to_ads_org_stats(df: DataFrame):
    """写入目标表ads_org_stats，增加类型校验和错误处理"""
    try:
        # 1. 校验目标表结构
        target_table = "tms_ads.ads_org_stats"
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


def execute_org_stats_etl(target_date: str):
    spark = get_spark_session()
    # 注意：这里使用传入的目标日期作为分区字段（20250725），其他表读取使用20250713
    print(f"[INFO] 开始执行机构统计ETL，目标分区日期：{target_date}，数据来源日期：20250713")
    source_date = "20250713"  # 其他表的字段日期

    # 1. 处理1天数据（1d表）
    # 1.1 订单数据
    org_order_1d = spark.table("tms_dws.dws_trade_org_cargo_type_order_1d") \
        .filter(col("dt") == source_date) \
        .groupBy("org_id", "org_name") \
        .agg(
        sum("order_count").alias("order_count"),
        sum("order_amount").alias("order_amount")
    ) \
        .withColumn("dt", lit(target_date)) \
        .withColumn("recent_days", lit(1))

    # 1.2 运输完成数据
    trans_1d = spark.table("tms_dws.dws_trans_org_truck_model_type_trans_finish_1d") \
        .filter(col("dt") == source_date) \
        .groupBy("org_id", "org_name") \
        .agg(
        sum("trans_finish_count").alias("trans_finish_count"),
        sum("trans_finish_distance").alias("trans_finish_distance"),
        sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
        (sum("trans_finish_distance") / sum("trans_finish_count")).alias("avg_trans_finish_distance"),
        (sum("trans_finish_dur_sec") / sum("trans_finish_count")).alias("avg_trans_finish_dur_sec")
    ) \
        .withColumn("dt", lit(target_date)) \
        .withColumn("recent_days", lit(1))

    # 1.3 1d部分关联
    part1d = org_order_1d \
        .join(trans_1d, on=["dt", "recent_days", "org_id", "org_name"], how="full_outer") \
        .select(
        coalesce(org_order_1d["dt"], trans_1d["dt"]).alias("dt"),
        coalesce(org_order_1d["recent_days"], trans_1d["recent_days"]).alias("recent_days"),
        coalesce(org_order_1d["org_id"], trans_1d["org_id"]).alias("org_id"),
        coalesce(org_order_1d["org_name"], trans_1d["org_name"]).alias("org_name"),
        col("order_count"),
        col("order_amount"),
        col("trans_finish_count"),
        col("trans_finish_distance"),
        col("trans_finish_dur_sec"),
        col("avg_trans_finish_distance"),
        col("avg_trans_finish_dur_sec")
    )

    # 2. 处理多天数据（nd表）
    # 2.1 订单数据
    org_order_nd = spark.table("tms_dws.dws_trade_org_cargo_type_order_nd") \
        .filter(col("dt") == source_date) \
        .groupBy("recent_days", "org_id", "org_name") \
        .agg(
        sum("order_count").alias("order_count"),
        sum("order_amount").alias("order_amount")
    ) \
        .withColumn("dt", lit(target_date))

    # 2.2 运输完成数据
    org_trans_nd = spark.table("tms_dws.dws_trans_shift_trans_finish_nd") \
        .filter(col("dt") == source_date) \
        .groupBy("recent_days", "org_id", "org_name") \
        .agg(
        sum("trans_finish_count").alias("trans_finish_count"),
        sum("trans_finish_distance").alias("trans_finish_distance"),
        sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
        (sum("trans_finish_distance") / sum("trans_finish_count")).alias("avg_trans_finish_distance"),
        (sum("trans_finish_dur_sec") / sum("trans_finish_count")).alias("avg_trans_finish_dur_sec")
    ) \
        .withColumn("dt", lit(target_date))

    # 2.3 nd部分关联
    partnd = org_order_nd \
        .join(org_trans_nd, on=["dt", "recent_days", "org_id", "org_name"], how="full_outer") \
        .select(
        coalesce(org_order_nd["dt"], org_trans_nd["dt"]).alias("dt"),
        coalesce(org_order_nd["recent_days"], org_trans_nd["recent_days"]).alias("recent_days"),
        coalesce(org_order_nd["org_id"], org_trans_nd["org_id"]).alias("org_id"),
        coalesce(org_order_nd["org_name"], org_trans_nd["org_name"]).alias("org_name"),
        col("order_count"),
        col("order_amount"),
        col("trans_finish_count"),
        col("trans_finish_distance"),
        col("trans_finish_dur_sec"),
        col("avg_trans_finish_distance"),
        col("avg_trans_finish_dur_sec")
    )

    # 3. 合并当前日期的新数据
    final_df = part1d.unionByName(partnd)

    # 4. 按目标表列顺序整理
    target_columns = [
        "dt", "recent_days", "org_id", "org_name",
        "order_count", "order_amount",
        "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec",
        "avg_trans_finish_distance", "avg_trans_finish_dur_sec"
    ]
    final_df = final_df.select(target_columns)

    # 5. 写入目标表
    write_to_ads_org_stats(final_df)
    print(f"[INFO] ETL执行完成，目标分区日期：{target_date}")


if __name__ == "__main__":
    target_date = '20250725'  # 写入表的分区字段
    execute_org_stats_etl(target_date)