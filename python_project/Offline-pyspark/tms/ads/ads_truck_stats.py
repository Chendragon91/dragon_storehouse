from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sum, col, coalesce, round, when
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType


def get_spark_session():
    """初始化SparkSession，使用tms数据库"""
    spark = SparkSession.builder \
        .appName("TruckStatsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark


def write_to_ads_truck_stats(df: DataFrame):
    """写入目标表ads_truck_stats，增加类型校验确保结构一致"""
    try:
        # 1. 读取目标表结构进行校验和对齐
        target_table = "tms_ads.ads_truck_stats"
        spark = get_spark_session()
        target_df = spark.table(target_table)

        # 2. 按目标表的列顺序和数据类型强制转换
        df = df.select([col(c).cast(target_df.schema[c].dataType) for c in target_df.columns])

        # 3. 写入数据（自动根据dt列值写入对应分区）
        df.write \
            .mode("append") \
            .insertInto(target_table)

        print(f"[INFO] 成功写入 {df.count()} 条新数据到 {target_table}")

    except Exception as e:
        print(f"[ERROR] 写入失败：{str(e)}")
        print("[ERROR] 待写入数据样例：")
        df.show(5, truncate=False)
        raise e


def execute_truck_stats_etl(target_date: str, source_date: str):
    spark = get_spark_session()
    print(f"[INFO] 开始执行卡车统计ETL，目标分区：{target_date}，源数据分区：{source_date}")

    # 1. 处理dws_trans_shift_trans_finish_nd数据（使用source_date作为分区条件）
    truck_stats_df = spark.table("tms_dws.dws_trans_shift_trans_finish_nd") \
        .filter(col("dt") == source_date) \
        .groupBy("truck_model_type", "truck_model_type_name", "recent_days") \
        .agg(
        coalesce(sum("trans_finish_count"), lit(0)).alias("trans_finish_count"),
        coalesce(sum("trans_finish_distance"), lit(0)).alias("trans_finish_distance"),
        coalesce(sum("trans_finish_dur_sec"), lit(0)).alias("trans_finish_dur_sec"),
        # 更安全的平均值计算（使用when避免除零错误）
        round(
            when(sum("trans_finish_count") == 0, lit(0.0))
            .otherwise(sum("trans_finish_distance") / sum("trans_finish_count")),
            2
        ).alias("avg_trans_finish_distance"),
        round(
            when(sum("trans_finish_count") == 0, lit(0))
            .otherwise(sum("trans_finish_dur_sec") / sum("trans_finish_count")),
            2
        ).alias("avg_trans_finish_dur_sec")
    ) \
        .withColumn("dt", lit(target_date))  \
    .withColumn("recent_days", col("recent_days").cast(IntegerType())) \
        .withColumn("truck_model_type", coalesce(col("truck_model_type"), lit(""))) \
        .withColumn("truck_model_type_name", coalesce(col("truck_model_type_name"), lit(""))) \
        .select(
        "dt", "recent_days", "truck_model_type", "truck_model_type_name",
        "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec",
        "avg_trans_finish_distance", "avg_trans_finish_dur_sec"
    )

    # 2. 写入前校验
    print("[INFO] 数据样例验证：")
    truck_stats_df.show(5, truncate=False)
    print(f"[INFO] 总记录数：{truck_stats_df.count()}")

    # 3. 写入目标表（自动写入target_date指定分区）
    write_to_ads_truck_stats(truck_stats_df)
    print(f"[INFO] ETL完成，数据已写入 {target_date} 分区")


if __name__ == "__main__":
    target_date = '20250725'  # 目标表分区日期
    source_date = '20250713'  # 源数据分区日期
    execute_truck_stats_etl(target_date, source_date)