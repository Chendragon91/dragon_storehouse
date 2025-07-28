from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum, explode, array

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("OrgReceiveStatsETL") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://cdh01:9083") \
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

def process_org_receive_stats(partition_date: str, table_name: str):
    """处理转运站揽收统计信息并写入Hive"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理转运站揽收统计信息，目标分区日期：{partition_date}")
    source_dt = "20250713"  # 源数据分区日期

    # 1. 读取源数据（源表分区为20250713）
    source_df = spark.table("tms_dws.dws_trans_org_receive_1d") \
        .filter(col("dt") == source_dt) \
        .select(
        "org_id",
        "org_name",
        "city_id",
        "city_name",
        "province_id",
        "province_name",
        "order_count",
        "order_amount"
    )

    # 2. 生成recent_days列（7和30天）并展开
    exploded_df = source_df.withColumn("recent_days", explode(array(lit(7), lit(30))))

    # 3. 按多维度分组聚合
    result_df = exploded_df.groupBy(
        "org_id",
        "org_name",
        "city_id",
        "city_name",
        "province_id",
        "province_name",
        "recent_days"
    ).agg(
        sum("order_count").alias("order_count"),
        sum("order_amount").alias("order_amount")
    ).withColumn("dt", lit(partition_date))  # 添加目标分区字段

    # 4. 确保字段类型匹配Hive表定义
    final_df = result_df.select(
        col("org_id").cast("bigint"),
        col("org_name"),
        col("city_id").cast("bigint"),
        col("city_name"),
        col("province_id").cast("bigint"),
        col("province_name"),
        col("recent_days").cast("tinyint"),
        col("order_count").cast("bigint"),
        col("order_amount").cast("decimal(16, 2)"),
        col("dt")
    )

    print("[INFO] 聚合结果预览：")
    final_df.show(5, truncate=False)

    # 5. 写入Hive
    write_to_hive(final_df, table_name, partition_date)

if __name__ == "__main__":
    target_table = 'dws_trans_org_receive_nd'
    target_partition = '20250725'  # 目标分区字段值
    process_org_receive_stats(target_partition, target_table)