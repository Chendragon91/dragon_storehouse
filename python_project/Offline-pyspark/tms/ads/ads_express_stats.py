from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sum, col, coalesce
from pyspark.sql import DataFrame


def get_spark_session():
    """初始化SparkSession，使用tms数据库"""
    spark = SparkSession.builder \
        .appName("TMSExpressStatsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark


def write_to_ads_express_stats(df: DataFrame, target_date: str):
    """写入目标表ads_express_stats，增加类型校验和错误处理"""
    try:
        # 1. 校验目标表结构
        target_table = "tms_ads.ads_express_stats"
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


def execute_express_stats_etl(source_date: str, target_date: str):
    spark = get_spark_session()
    print(f"[INFO] 开始执行快递统计ETL，源数据日期：{source_date}，目标分区日期：{target_date}")

    # 1. 处理1天数据（1d表）
    # 1.1 派送成功数据
    deliver_1d = spark.table("tms_dws.dws_trans_org_deliver_suc_1d") \
        .filter(col("dt") == source_date) \
        .agg(sum("order_count").alias("deliver_suc_count")) \
        .withColumn("dt", lit(source_date)) \
        .withColumn("recent_days", lit(1))

    # 1.2 分拣数据
    sort_1d = spark.table("tms_dws.dws_trans_org_sort_1d") \
        .filter(col("dt") == source_date) \
        .agg(sum("sort_count").alias("sort_count")) \
        .withColumn("dt", lit(source_date)) \
        .withColumn("recent_days", lit(1))

    # 1.3 1d部分全外连接
    part1d = deliver_1d \
        .join(sort_1d, on=["dt", "recent_days"], how="full_outer") \
        .select(
        coalesce(deliver_1d["dt"], sort_1d["dt"]).alias("dt"),
        coalesce(deliver_1d["recent_days"], sort_1d["recent_days"]).alias("recent_days"),
        col("deliver_suc_count"),
        col("sort_count")
    )

    # 2. 处理多天数据（nd表）
    # 2.1 派送成功数据
    deliver_nd = spark.table("tms_dws.dws_trans_org_deliver_suc_nd") \
        .filter(col("dt") == source_date) \
        .groupBy("recent_days") \
        .agg(sum("order_count").alias("deliver_suc_count")) \
        .withColumn("dt", lit(source_date))

    # 2.2 分拣数据
    sort_nd = spark.table("tms_dws.dws_trans_org_sort_nd") \
        .filter(col("dt") == source_date) \
        .groupBy("recent_days") \
        .agg(sum("sort_count").alias("sort_count")) \
        .withColumn("dt", lit(source_date))

    # 2.3 nd部分全外连接
    partnd = deliver_nd \
        .join(sort_nd, on=["dt", "recent_days"], how="full_outer") \
        .select(
        coalesce(deliver_nd["dt"], sort_nd["dt"]).alias("dt"),
        coalesce(deliver_nd["recent_days"], sort_nd["recent_days"]).alias("recent_days"),
        col("deliver_suc_count"),
        col("sort_count")
    )

    # 3. 合并所有数据
    final_df = part1d.unionByName(partnd)

    # 4. 更新dt为目标分区日期
    final_df = final_df.withColumn("dt", lit(target_date))

    # 5. 按目标表列顺序整理
    target_columns = ["dt", "recent_days", "deliver_suc_count", "sort_count"]
    final_df = final_df.select(target_columns)

    # 6. 写入目标表
    write_to_ads_express_stats(final_df, target_date)
    print(f"[INFO] ETL执行完成，源数据日期：{source_date}，目标分区日期：{target_date}")


if __name__ == "__main__":
    source_date = '20250713'  # 源数据日期
    target_date = '20250725'  # 目标分区日期
    execute_express_stats_etl(source_date, target_date)