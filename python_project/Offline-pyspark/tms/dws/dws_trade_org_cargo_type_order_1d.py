from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, count, sum, max

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("OrgOrderStatsETL") \
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
    jdbcDF.drop("ds").write \
        .mode('append') \
        .insertInto(f"tms_dws.{tableName}")

def process_org_order_stats(partition_date: str, tableName):
    """处理机构订单统计信息并写入Hive"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理机构订单统计信息，目标分区日期：{partition_date}")
    source_dt = "20250713"  # 源数据分区日期

    # 1. 读取订单明细数据
    order_detail = spark.table("tms_dwd.dwd_trade_order_detail_inc") \
        .select(
        col("order_id"),
        col("cargo_type"),
        col("cargo_type_name"),
        col("sender_district_id"),
        col("sender_city_id"),
        col("amount"),
        col("dt")
    ).alias("detail")

    # 2. 按订单去重并保留最大金额
    distinct_detail = order_detail.groupBy(
        "order_id", "cargo_type", "cargo_type_name",
        "sender_district_id", "sender_city_id", "dt"
    ).agg(
        max("amount").alias("amount")
    ).alias("distinct_detail")

    # 3. 读取机构维度数据
    dim_org = spark.table("tms_dim.dim_organ_full") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id").alias("org_id"),
        col("org_name"),
        col("region_id")
    ).alias("org")

    # 4. 关联机构数据并聚合统计
    agg_stats = distinct_detail.join(
        dim_org,
        col("distinct_detail.sender_district_id") == col("org.region_id"),
        "left"
    ).groupBy(
        "org_id", "org_name", "cargo_type",
        "cargo_type_name", "sender_city_id", "dt"
    ).agg(
        count("order_id").alias("order_count"),
        sum("amount").alias("order_amount")
    ).alias("agg")

    # 5. 读取地区维度数据
    dim_region = spark.table("tms_dim.dim_region_full") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id"),
        col("name")
    ).alias("region")

    # 6. 最终关联查询（修复city_id类型不匹配问题）
    result_df = agg_stats.join(
        dim_region,
        col("agg.sender_city_id") == col("region.id"),
        "left"
    ).select(
        col("agg.org_id"),
        col("agg.org_name"),
        # 将sender_city_id转换为BIGINT以匹配Hive表定义
        col("agg.sender_city_id").cast("BIGINT").alias("city_id"),
        col("region.name").alias("city_name"),
        col("agg.cargo_type"),
        col("agg.cargo_type_name"),
        col("agg.order_count"),
        col("agg.order_amount"),
        col("agg.dt")
    )

    # 添加目标分区字段
    final_df = result_df.withColumn("ds", lit(partition_date))

    print(f"[INFO] 数据处理完成，准备写入分区{partition_date}")
    final_df.show(5)  # 预览5条数据

    # 写入Hive
    select_to_hive(final_df, tableName, partition_date)

if __name__ == "__main__":
    target_table = 'dws_trade_org_cargo_type_order_1d'  # 目标表名
    target_partition = '20250725'  # 目标分区字段值
    process_org_order_stats(target_partition, target_table)