from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum, explode, array

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession（修正Metastore配置）"""
    spark = SparkSession.builder \
        .appName("DispatchStatsETL") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://cdh01:9083")  \
    .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")  # 指定Hive数据库
    return spark

def write_to_hive(df, table_name, partition_date):
    """将DataFrame写入Hive分区表（带异常处理）"""
    try:
        df.write \
            .mode('append') \
            .insertInto(f"tms_dws.{table_name}")
        print(f"[SUCCESS] 数据成功写入分区 {partition_date}")
    except Exception as e:
        print(f"[ERROR] 写入Hive失败: {str(e)}")
        raise

def process_dispatch_stats(partition_date: str, table_name: str):
    """主处理逻辑：统计发单数据并写入Hive"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理发单统计信息，目标分区日期：{partition_date}")

    # 1. 读取源数据（源表分区为20250713）
    source_df = spark.table("tms_dws.dws_trans_dispatch_1d") \
        .filter(col("dt") == "20250713") \
        .select("order_count", "order_amount")

    # 2. 生成recent_days列（7和30天）并展开
    exploded_df = source_df.withColumn("recent_days", explode(array(lit(7), lit(30))))

    # 3. 按recent_days聚合统计
    result_df = exploded_df.groupBy("recent_days") \
        .agg(
        sum("order_count").alias("order_count"),
        sum("order_amount").alias("order_amount")
    ) \
        .withColumn("dt", lit(partition_date))  # 添加目标分区字段

    # 4. 显示结果（调试用）
    print("[INFO] 聚合结果预览：")
    result_df.show(5, truncate=False)

    # 5. 写入Hive
    write_to_hive(result_df, table_name, partition_date)

if __name__ == "__main__":
    # 目标配置
    target_table = 'dws_trans_dispatch_nd'
    target_partition = '20250725'  # 与SQL中的20250713区分

    # 执行处理
    process_dispatch_stats(target_partition, target_table)