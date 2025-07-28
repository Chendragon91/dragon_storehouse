from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, count, sum, max
from pyspark.sql.types import DecimalType

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("DispatchStatsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "false") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark

def select_to_hive(jdbcDF, tableName, partition_date):
    """将DataFrame写入Hive分区表"""
    try:
        # 按照目标表结构调整列顺序和类型
        final_data = jdbcDF.select(
            col("order_count").cast("bigint").alias("order_count"),
            col("order_amount").cast(DecimalType(16,2)).alias("order_amount"),
            col("dt").cast("string").alias("dt")
        )

        # 打印调试信息
        print("=== 写入前的最终数据 ===")
        final_data.show(5, truncate=False)
        print("=== 写入前的Schema ===")
        final_data.printSchema()

        # 写入Hive（确保列顺序与目标表完全一致）
        final_data.write \
            .mode('append') \
            .insertInto(f"tms_dws.{tableName}")

        print(f"[SUCCESS] 数据成功写入表 tms_dws.{tableName} 分区 {partition_date}")
    except Exception as e:
        print(f"[ERROR] 写入Hive表失败: {str(e)}")
        raise

def process_dispatch_stats(partition_date: str, tableName):
    """处理调度统计信息并写入Hive"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理调度统计信息，目标分区日期：{partition_date}")

    try:
        # 1. 检查目标表是否存在
        if not spark.catalog.tableExists(f"tms_dws.{tableName}"):
            raise Exception(f"目标表 tms_dws.{tableName} 不存在")

        # 2. 读取调度明细数据
        print("[INFO] 正在读取源表数据...")
        df = spark.table("tms_dwd.dwd_trans_dispatch_detail_inc")

        # 3. 按订单和日期去重
        print("[INFO] 正在处理数据去重...")
        distinct_info = df.filter(col("dt") == partition_date) \
            .groupBy("order_id", "dt") \
            .agg(max("amount").alias("distinct_amount"))

        # 4. 按日期分组计算指标
        print("[INFO] 正在计算聚合指标...")
        result_df = distinct_info.groupBy("dt") \
            .agg(
            count("order_id").alias("order_count"),
            sum("distinct_amount").alias("order_amount")
        ).withColumn("ds", lit(partition_date))

        # 5. 确保数据类型正确
        print("[INFO] 正在转换数据类型...")
        final_df = result_df.select(
            col("order_count").cast("bigint").alias("order_count"),
            col("order_amount").cast(DecimalType(16,2)).alias("order_amount"),
            col("dt").cast("string").alias("dt"),
            col("ds").cast("string").alias("ds")
        )

        # 6. 打印调试信息
        print("[INFO] 数据处理完成，准备写入Hive")
        print("=== 处理后的数据预览 ===")
        final_df.show(5, truncate=False)
        print("=== 处理后的Schema ===")
        final_df.printSchema()

        # 7. 写入Hive
        select_to_hive(final_df, tableName, partition_date)

    except Exception as e:
        print(f"[ERROR] 处理数据时出错: {str(e)}")
        raise
    finally:
        spark.stop()
        print("[INFO] Spark会话已关闭")

if __name__ == "__main__":
    target_table = 'dws_trans_dispatch_1d'
    target_partition = '20250725'

    print("=== 开始执行调度统计ETL作业 ===")
    process_dispatch_stats(target_partition, target_table)
    print("=== 作业执行完成 ===")