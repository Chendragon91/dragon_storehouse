from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sum, col, coalesce, round
from pyspark.sql import DataFrame
import time

def get_spark_session():
    """初始化SparkSession，使用tms数据库"""
    spark = SparkSession.builder \
        .appName("TMSExpressCityStatsETL") \
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

def write_to_ads_express_city_stats(df: DataFrame):
    """写入目标表ads_express_city_stats，增加类型校验和错误处理"""
    try:
        # 1. 校验目标表结构
        target_table = "tms_ads.ads_express_city_stats"
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

def execute_express_city_stats_etl(target_date: str):
    spark = None
    try:
        spark = get_spark_session()
        print(f"[INFO] 开始执行快递城市统计ETL，目标日期：{target_date}")

        # 1. 处理历史数据（直接从ads表获取）
        history_df = spark.table("tms_ads.ads_express_city_stats") \
            .select(
            "dt", "recent_days", "city_id", "city_name",
            "receive_order_count", "receive_order_amount",
            "deliver_suc_count", "sort_count"
        ).cache()

        # 2. 处理1天数据（1d表）
        # 2.1 派送成功数据
        city_deliver_1d = spark.table("tms_dws.dws_trans_org_deliver_suc_1d") \
            .filter(col("dt") == target_date) \
            .groupBy("city_id", "city_name") \
            .agg(sum("order_count").alias("deliver_suc_count")) \
            .withColumn("dt", lit(target_date)) \
            .withColumn("recent_days", lit(1)).cache()

        # 2.2 分拣数据
        city_sort_1d = spark.table("tms_dws.dws_trans_org_sort_1d") \
            .filter(col("dt") == target_date) \
            .groupBy("city_id", "city_name") \
            .agg(sum("sort_count").alias("sort_count")) \
            .withColumn("dt", lit(target_date)) \
            .withColumn("recent_days", lit(1)).cache()

        # 2.3 揽收数据（金额保留两位小数）
        city_receive_1d = spark.table("tms_dws.dws_trans_org_receive_1d") \
            .filter(col("dt") == target_date) \
            .groupBy("city_id", "city_name") \
            .agg(
            sum("order_count").alias("receive_order_count"),
            round(sum("order_amount"), 2).alias("receive_order_amount")
        ) \
            .withColumn("dt", lit(target_date)) \
            .withColumn("recent_days", lit(1)).cache()

        # 2.4 1d部分关联
        part1d = city_deliver_1d \
            .join(city_sort_1d, on=["dt", "recent_days", "city_id", "city_name"], how="full_outer") \
            .join(city_receive_1d, on=["dt", "recent_days", "city_id", "city_name"], how="full_outer") \
            .select(
            coalesce(city_deliver_1d["dt"], city_sort_1d["dt"], city_receive_1d["dt"]).alias("dt"),
            coalesce(city_deliver_1d["recent_days"], city_sort_1d["recent_days"], city_receive_1d["recent_days"]).alias("recent_days"),
            coalesce(city_deliver_1d["city_id"], city_sort_1d["city_id"], city_receive_1d["city_id"]).alias("city_id"),
            coalesce(city_deliver_1d["city_name"], city_sort_1d["city_name"], city_receive_1d["city_name"]).alias("city_name"),
            col("receive_order_count"),
            col("receive_order_amount"),
            col("deliver_suc_count"),
            col("sort_count")
        ).cache()

        # 3. 处理多天数据（nd表）
        # 3.1 派送成功数据
        city_deliver_nd = spark.table("tms_dws.dws_trans_org_deliver_suc_nd") \
            .filter(col("dt") == target_date) \
            .groupBy("recent_days", "city_id", "city_name") \
            .agg(sum("order_count").alias("deliver_suc_count")) \
            .withColumn("dt", lit(target_date)).cache()

        # 3.2 分拣数据
        city_sort_nd = spark.table("tms_dws.dws_trans_org_sort_nd") \
            .filter(col("dt") == target_date) \
            .groupBy("recent_days", "city_id", "city_name") \
            .agg(sum("sort_count").alias("sort_count")) \
            .withColumn("dt", lit(target_date)).cache()

        # 3.3 揽收数据
        city_receive_nd = spark.table("tms_dws.dws_trans_org_receive_nd") \
            .filter(col("dt") == target_date) \
            .groupBy("recent_days", "city_id", "city_name") \
            .agg(
            sum("order_count").alias("receive_order_count"),
            round(sum("order_amount"), 2).alias("receive_order_amount")
        ) \
            .withColumn("dt", lit(target_date)).cache()

        # 3.4 nd部分关联
        partnd = city_deliver_nd \
            .join(city_sort_nd, on=["dt", "recent_days", "city_id", "city_name"], how="full_outer") \
            .join(city_receive_nd, on=["dt", "recent_days", "city_id", "city_name"], how="full_outer") \
            .select(
            coalesce(city_deliver_nd["dt"], city_sort_nd["dt"], city_receive_nd["dt"]).alias("dt"),
            coalesce(city_deliver_nd["recent_days"], city_sort_nd["recent_days"], city_receive_nd["recent_days"]).alias("recent_days"),
            coalesce(city_deliver_nd["city_id"], city_sort_nd["city_id"], city_receive_nd["city_id"]).alias("city_id"),
            coalesce(city_deliver_nd["city_name"], city_sort_nd["city_name"], city_receive_nd["city_name"]).alias("city_name"),
            col("receive_order_count"),
            col("receive_order_amount"),
            col("deliver_suc_count"),
            col("sort_count")
        ).cache()

        # 4. 合并当前日期的新数据（不包含历史数据，避免重复写入）
        final_df = history_df.unionByName(part1d).unionByName(partnd).cache()

        # 5. 按目标表列顺序整理
        target_columns = [
            "dt", "recent_days", "city_id", "city_name",
            "receive_order_count", "receive_order_amount",
            "deliver_suc_count", "sort_count"
        ]
        final_df = final_df.select(target_columns)

        # 6. 写入目标表
        write_to_ads_express_city_stats(final_df)
        print(f"[INFO] ETL执行完成，目标日期：{target_date}")

    except Exception as e:
        print(f"[ERROR] ETL执行失败: {str(e)}")
        raise e
    finally:
        # 清理缓存
        try:
            history_df.unpersist()
            city_deliver_1d.unpersist()
            city_sort_1d.unpersist()
            city_receive_1d.unpersist()
            part1d.unpersist()
            city_deliver_nd.unpersist()
            city_sort_nd.unpersist()
            city_receive_nd.unpersist()
            partnd.unpersist()
            final_df.unpersist()
        except:
            pass
        # 关闭SparkSession
        if spark:
            spark.stop()

if __name__ == "__main__":
    target_date = '20250713'  # 可通过调度工具动态传入（如Airflow/Oozie）
    execute_express_city_stats_etl(target_date)