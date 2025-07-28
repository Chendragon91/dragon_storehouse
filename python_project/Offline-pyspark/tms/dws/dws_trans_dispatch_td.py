from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as _sum

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("TransDispatchStatsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark

def write_to_hive(df, table_name, partition_date):
    """将DataFrame写入Hive分区表"""
    df.write \
        .mode('append') \
        .insertInto(f"tms_dws.{table_name}")

def process_trans_dispatch_stats(partition_date: str, table_name: str):
    """处理物流域发单历史至今汇总统计并写入Hive"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理物流域发单历史至今汇总统计，目标分区日期：{partition_date}")

    # 1. 读取源数据表
    dispatch_stats = spark.table("tms_dws.dws_trans_dispatch_1d") \
        .filter(col("dt") == "20250713")  # 使用ods层的分区日期

    # 2. 聚合计算
    agg_stats = dispatch_stats.agg(
        _sum("order_count").alias("order_count"),
        _sum("order_amount").alias("order_amount")
    )

    # 3. 添加目标分区字段
    final_df = agg_stats.withColumn("dt", lit(partition_date))

    print(f"[INFO] 数据处理完成，准备写入分区{partition_date}")
    final_df.show(5)  # 预览数据

    # 4. 写入Hive
    write_to_hive(final_df, table_name, partition_date)

if __name__ == "__main__":
    target_table = 'dws_trans_dispatch_td'  # 目标表名
    target_partition = '20250725'  # 目标分区字段值
    process_trans_dispatch_stats(target_partition, target_table)