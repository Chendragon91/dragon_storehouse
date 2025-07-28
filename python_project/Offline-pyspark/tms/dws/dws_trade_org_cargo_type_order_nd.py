from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum, explode, array

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("OrgCargoTypeStatsETL") \
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
    jdbcDF.withColumn("dt", lit(partition_date)).write \
        .mode('append') \
        .insertInto(f"tms_dws.{tableName}")

def process_org_cargo_type_stats(partition_date: str, tableName):
    """处理机构货品类型统计信息并写入Hive"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理机构货品类型统计信息，目标分区日期：{partition_date}")
    source_dt = "20250713"  # 源数据分区日期

    # 1. 读取DWS层机构货品类型订单日统计表
    dws_stats = spark.table("tms_dws.dws_trade_org_cargo_type_order_1d") \
        .filter(col("dt") == partition_date) \
    .select(
        col("org_id"),
        col("org_name"),
        col("city_id"),
        col("city_name"),
        col("cargo_type"),
        col("cargo_type_name"),
        col("order_count"),
        col("order_amount"),
        col("dt")
    ).alias("dws_stats")

    # 2. 使用explode展开recent_days数组(7,30)
    exploded_stats = dws_stats.withColumn(
        "recent_days",
        explode(array(lit(7), lit(30)))
    ).alias("exploded_stats")

    # 3. 按机构、城市、货品类型和天数聚合统计
    result_df = exploded_stats.groupBy(
        "org_id",
        "org_name",
        "city_id",
        "city_name",
        "cargo_type",
        "cargo_type_name",
        "recent_days"
    ).agg(
        sum("order_count").alias("order_count"),
        sum("order_amount").alias("order_amount")
    ).select(
        col("org_id"),
        col("org_name"),
        col("city_id"),
        col("city_name"),
        col("cargo_type"),
        col("cargo_type_name"),
        col("recent_days"),
        col("order_count"),
        col("order_amount")
    )

    # 添加目标分区字段
    final_df = result_df.withColumn("dt", lit(partition_date))  # 修改为使用dt作为分区列

    print(f"[INFO] 数据处理完成，准备写入分区{partition_date}")
    final_df.show(5)  # 预览5条数据

    # 写入Hive
    select_to_hive(final_df, tableName, partition_date)

if __name__ == "__main__":
    target_table = 'dws_trade_org_cargo_type_order_nd'  # 目标表名
    target_partition = '20250725'  # 目标分区字段值
    process_org_cargo_type_stats(target_partition, target_table)