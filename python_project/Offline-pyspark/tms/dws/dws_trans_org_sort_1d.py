from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, count, when

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("TransOrgSortETL") \
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

def process_org_sort_stats(partition_date: str, table_name: str):
    """处理机构分拣统计信息并写入Hive"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理机构分拣统计，目标分区日期：{partition_date}")
    source_dt = "20250713"  # 源数据分区日期

    # 1. 读取分拣明细数据并聚合
    agg_stats = spark.table("tms_dwd.dwd_bound_sort_inc") \
        .filter(col("dt") == source_dt) \
        .groupBy("org_id", "dt") \
        .agg(count("*").alias("sort_count")) \
        .alias("agg")

    # 2. 读取机构维度数据
    dim_org = spark.table("tms_dim.dim_organ_full") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id"),
        col("org_name"),
        col("org_level"),
        col("region_id")
    ).alias("org")

    # 3. 读取地区维度数据（城市级别）
    dim_city = spark.table("tms_dim.dim_region_full") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id"),
        col("name"),
        col("parent_id")  # 省份ID
    ).alias("city_for_level1")

    # 4. 读取地区维度数据（省份级别）
    dim_province1 = spark.table("tms_dim.dim_region_full") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id"),
        col("name"),
        col("parent_id")  # 上级省份ID（用于level2）
    ).alias("province_for_level1")

    # 5. 读取地区维度数据（上级省份级别）
    dim_province2 = spark.table("tms_dim.dim_region_full") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id"),
        col("name")
    ).alias("province_for_level2")

    # 6. 执行多表关联
    joined_df = agg_stats.join(
        dim_org,
        col("agg.org_id") == col("org.id"),
        "left"
    ).join(
        dim_city,
        col("org.region_id") == col("city_for_level1.id"),
        "left"
    ).join(
        dim_province1,
        col("city_for_level1.parent_id") == col("province_for_level1.id"),
        "left"
    ).join(
        dim_province2,
        col("province_for_level1.parent_id") == col("province_for_level2.id"),
        "left"
    )

    # 7. 处理机构级别逻辑
    result_df = joined_df.select(
        col("agg.org_id"),
        col("org.org_name"),
        # 根据机构级别选择城市ID
        when(col("org.org_level") == 1, col("city_for_level1.id"))
        .otherwise(col("province_for_level1.id"))
        .alias("city_id"),
        # 根据机构级别选择城市名称
        when(col("org.org_level") == 1, col("city_for_level1.name"))
        .otherwise(col("province_for_level1.name"))
        .alias("city_name"),
        # 根据机构级别选择省份ID
        when(col("org.org_level") == 1, col("province_for_level1.id"))
        .otherwise(col("province_for_level2.id"))
        .alias("province_id"),
        # 根据机构级别选择省份名称
        when(col("org.org_level") == 1, col("province_for_level1.name"))
        .otherwise(col("province_for_level2.name"))
        .alias("province_name"),
        col("agg.sort_count"),
        col("agg.dt")
    )

    # 添加目标分区字段
    final_df = result_df.withColumn("dt", lit(partition_date))

    print(f"[INFO] 数据处理完成，准备写入分区{partition_date}")
    final_df.show(5)  # 预览5条数据

    # 写入Hive
    write_to_hive(final_df, table_name, partition_date)

if __name__ == "__main__":
    target_table = 'dws_trans_org_sort_1d'  # 目标表名
    target_partition = '20250725'  # 目标分区字段值
    process_org_sort_stats(target_partition, target_table)