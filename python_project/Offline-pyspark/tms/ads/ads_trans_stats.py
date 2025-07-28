from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sum, col, coalesce
from pyspark.sql import DataFrame


def get_spark_session():
    """初始化SparkSession，使用tms数据库"""
    spark = SparkSession.builder \
        .appName("TMSTransStatsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark


def write_to_ads_trans_stats(df: DataFrame, target_date: str):
    """写入目标表ads_trans_stats，增加类型校验和错误处理"""
    try:
        # 1. 校验目标表结构
        target_table = "tms_ads.ads_trans_stats"
        spark = get_spark_session()
        target_df = spark.table(target_table)

        # 2. 按目标表列顺序和类型调整DataFrame
        df = df.select([col(c).cast(target_df.schema[c].dataType) for c in target_df.columns])

        # 3. 写入目标表（分区为20250725）
        df.write \
            .mode("append") \
            .insertInto(target_table)

        print(f"[INFO] 成功写入 {df.count()} 条新数据到 {target_table}，分区日期：{target_date}")

    except Exception as e:
        print(f"[ERROR] 写入失败：{str(e)}")
        print("[ERROR] 待写入数据样例：")
        df.show(5)
        raise e  # 抛出异常，终止作业


def execute_trans_stats_etl(source_date: str, target_date: str):
    spark = get_spark_session()
    print(f"[INFO] 开始执行运输综合统计ETL，源数据日期：{source_date}，目标分区日期：{target_date}")

    # 1. 处理1天数据（1d表）
    trans_1d = spark.table("tms_dws.dws_trans_org_truck_model_type_trans_finish_1d") \
        .filter(col("dt") == source_date) \
        .agg(
        sum("trans_finish_count").alias("trans_finish_count"),
        sum("trans_finish_distance").alias("trans_finish_distance"),
        sum("trans_finish_dur_sec").alias("trans_finish_dur_sec")
    ) \
        .withColumn("dt", lit(target_date)) \
        .withColumn("recent_days", lit(1))

    # 2. 处理多天数据（nd表）
    trans_nd = spark.table("tms_dws.dws_trans_shift_trans_finish_nd") \
        .filter(col("dt") == source_date) \
        .groupBy("recent_days") \
        .agg(
        sum("trans_finish_count").alias("trans_finish_count"),
        sum("trans_finish_distance").alias("trans_finish_distance"),
        sum("trans_finish_dur_sec").alias("trans_finish_dur_sec")
    ) \
        .withColumn("dt", lit(target_date))

    # 3. 合并所有数据
    final_df = trans_1d.unionByName(trans_nd)

    # 4. 按目标表列顺序整理
    target_columns = [
        "dt", "recent_days",
        "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec"
    ]
    final_df = final_df.select(target_columns)

    # 5. 写入目标表
    write_to_ads_trans_stats(final_df, target_date)
    print(f"[INFO] ETL执行完成，源数据日期：{source_date}，目标分区日期：{target_date}")


if __name__ == "__main__":
    source_date = '20250713'  # 源数据日期
    target_date = '20250725'  # 目标分区日期
    execute_trans_stats_etl(source_date, target_date)