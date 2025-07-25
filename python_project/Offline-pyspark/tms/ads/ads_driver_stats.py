from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sum, col, coalesce, round, when, array, explode
from pyspark.sql import DataFrame
import time

def get_spark_session():
    """初始化SparkSession，使用tms数据库"""
    spark = SparkSession.builder \
        .appName("TMSDriverStatsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.sql.shuffle.partitions", "200") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark

def write_to_ads_driver_stats(df: DataFrame):
    """写入目标表ads_driver_stats，增加类型校验和错误处理"""
    try:
        # 1. 校验目标表结构
        target_table = "tms_ads.ads_driver_stats"
        spark = get_spark_session()
        target_df = spark.table(target_table)

        # 2. 按目标表列顺序和类型调整DataFrame
        df = df.select([col(c).cast(target_df.schema[c].dataType) for c in target_df.columns])

        # 3. 写入前缓存数据
        df.cache()
        print(f"[INFO] 准备写入 {df.count()} 条数据到 {target_table}")

        # 4. 分批写入
        try:
            df.write \
                .mode("append") \
                .insertInto(target_table)
            print(f"[INFO] 成功写入数据到 {target_table}")
        except Exception as e:
            # 尝试重新写入
            print(f"[WARN] 首次写入失败，尝试重新写入: {str(e)}")
            time.sleep(10)  # 等待10秒
            df.write \
                .mode("append") \
                .insertInto(target_table)
            print(f"[INFO] 重试写入成功")

    except Exception as e:
        print(f"[ERROR] 写入失败：{str(e)}")
        print("[ERROR] 待写入数据样例：")
        try:
            df.limit(5).show(truncate=False)
        except:
            print("无法显示数据样例")
        raise e  # 抛出异常，终止作业
    finally:
        # 释放缓存
        try:
            df.unpersist()
        except:
            pass

def execute_driver_stats_etl(target_date: str):
    spark = None
    try:
        spark = get_spark_session()
        print(f"[INFO] 开始执行司机统计ETL，目标日期：{target_date}")

        # 1. 处理历史数据（直接从ads表获取）
        history_df = spark.table("tms_ads.ads_driver_stats") \
            .select(
            "dt", "recent_days",
            col("driver_emp_id").alias("driver_id"),
            "driver_name",
            "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec",
            "avg_trans_finish_distance", "avg_trans_finish_dur_sec", "trans_finish_late_count"
        ).cache()

        # 2. 处理新数据（从dws表计算）
        # 2.1 处理单车司机数据
        single_driver = spark.table("tms_dws.dws_trans_shift_trans_finish_nd") \
            .filter((col("dt") == target_date) & (col("driver2_emp_id").isNull())) \
            .select(
            "recent_days",
            col("driver1_emp_id").alias("driver_id"),
            col("driver1_name").alias("driver_name"),
            "trans_finish_count",
            "trans_finish_distance",
            "trans_finish_dur_sec",
            col("trans_finish_delay_count").alias("trans_finish_late_count")
        ).cache()

        # 2.2 处理双车司机数据（需要拆分统计）
        double_driver = spark.table("tms_dws.dws_trans_shift_trans_finish_nd") \
            .filter((col("dt") == target_date) & (col("driver2_emp_id").isNotNull())) \
            .select(
            "recent_days",
            array(
                array(col("driver1_emp_id"), col("driver1_name")),
                array(col("driver2_emp_id"), col("driver2_name"))
            ).alias("driver_arr"),
            col("trans_finish_count"),
            (col("trans_finish_distance") / 2).alias("trans_finish_distance"),
            (col("trans_finish_dur_sec") / 2).alias("trans_finish_dur_sec"),
            col("trans_finish_delay_count").alias("trans_finish_late_count")
        ) \
            .select(
            "recent_days",
            explode("driver_arr").alias("driver_info"),
            "trans_finish_count",
            "trans_finish_distance",
            "trans_finish_dur_sec",
            "trans_finish_late_count"
        ) \
            .select(
            "recent_days",
            col("driver_info")[0].cast("bigint").alias("driver_id"),
            col("driver_info")[1].alias("driver_name"),
            "trans_finish_count",
            "trans_finish_distance",
            "trans_finish_dur_sec",
            "trans_finish_late_count"
        ).cache()

        # 2.3 合并司机数据并聚合
        driver_stats = single_driver.unionByName(double_driver) \
            .groupBy("recent_days", "driver_id", "driver_name") \
            .agg(
            sum("trans_finish_count").alias("trans_finish_count"),
            sum("trans_finish_distance").alias("trans_finish_distance"),
            sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
            (sum("trans_finish_distance") / sum("trans_finish_count")).alias("avg_trans_finish_distance"),
            (sum("trans_finish_dur_sec") / sum("trans_finish_count")).alias("avg_trans_finish_dur_sec"),
            sum("trans_finish_late_count").alias("trans_finish_late_count")
        ) \
            .withColumn("dt", lit(target_date)) \
            .select(
            "dt", "recent_days", "driver_id", "driver_name",
            "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec",
            "avg_trans_finish_distance", "avg_trans_finish_dur_sec", "trans_finish_late_count"
        ).cache()

        # 3. 合并历史数据和新数据
        final_df = history_df.unionByName(driver_stats)

        # 4. 按目标表列顺序整理（将driver_id重命名为driver_emp_id以匹配目标表）
        final_df = final_df.withColumnRenamed("driver_id", "driver_emp_id")

        target_columns = [
            "dt", "recent_days", "driver_emp_id", "driver_name",
            "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec",
            "avg_trans_finish_distance", "avg_trans_finish_dur_sec", "trans_finish_late_count"
        ]
        final_df = final_df.select(target_columns).cache()

        # 5. 写入目标表
        write_to_ads_driver_stats(final_df)
        print(f"[INFO] ETL执行完成，目标日期：{target_date}")

    except Exception as e:
        print(f"[ERROR] ETL执行失败: {str(e)}")
        raise e
    finally:
        # 清理缓存
        try:
            history_df.unpersist()
            single_driver.unpersist()
            double_driver.unpersist()
            driver_stats.unpersist()
            final_df.unpersist()
        except:
            pass
        # 关闭SparkSession
        if spark:
            spark.stop()

if __name__ == "__main__":
    target_date = '20250713'
    execute_driver_stats_etl(target_date)