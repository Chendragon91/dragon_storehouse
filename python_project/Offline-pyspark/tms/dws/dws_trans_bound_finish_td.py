from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, count, sum, max

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("BoundFinishStatsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark

def select_to_hive(jdbcDF, tableName, partition_date):
    """将DataFrame写入Hive分区表"""
    jdbcDF.drop("dt").write \
        .mode('append') \
        .insertInto(f"tms_dws.{tableName}")

def process_bound_finish_stats(partition_date: str, tableName):
    """处理运单完成统计信息并写入Hive"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理运单完成统计信息，目标分区日期：{partition_date}")
    source_dt = "20250713"  # 源数据分区日期

    # 1. 读取运单完成明细数据并按订单去重
    distinct_info = spark.table("tms_dwd.dwd_trans_bound_finish_detail_inc") \
        .groupBy("order_id") \
        .agg(
        max("amount").alias("order_amount")
    ).alias("distinct_info")

    # 2. 计算总订单数和总金额
    result_df = distinct_info.agg(
        count("order_id").alias("order_count"),
        sum("order_amount").alias("order_amount")
    )

    # 添加目标分区字段
    final_df = result_df.withColumn("ds", lit(partition_date))

    print(f"[INFO] 数据处理完成，准备写入分区{partition_date}")
    final_df.show(5)  # 预览数据

    # 写入Hive
    select_to_hive(final_df, tableName, partition_date)

if __name__ == "__main__":
    target_table = 'dws_trans_bound_finish_td'  # 目标表名
    target_partition = '20250725'  # 目标分区字段值
    process_bound_finish_stats(target_partition, target_table)