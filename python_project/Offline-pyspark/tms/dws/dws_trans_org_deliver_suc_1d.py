from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, count

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("TransOrgDeliverSucETL") \
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

def process_org_deliver_suc_stats(partition_date: str, table_name: str):
    """处理转运站派送成功统计信息并写入Hive"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理转运站派送成功统计，目标分区日期：{partition_date}")
    source_dt = "20250713"  # 源数据分区日期

    # 1. 读取派送成功明细数据并按订单去重
    deliver_detail = spark.table("tms_dwd.dwd_trans_deliver_suc_detail_inc") \
        .filter(col("dt") == source_dt) \
        .groupBy("order_id", "receiver_district_id", "dt") \
        .count() \
        .select("order_id", "receiver_district_id", "dt") \
        .alias("detail")

    # 2. 读取机构维度数据
    dim_org = spark.table("tms_dim.dim_organ_full") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id").alias("org_id"),
        col("org_name"),
        col("region_id").alias("district_id")
    ).alias("organ")

    # 3. 读取地区维度数据（区县级别）
    dim_district = spark.table("tms_dim.dim_region_full") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id"),
        col("parent_id").alias("city_id")
    ).alias("district")

    # 4. 读取地区维度数据（城市级别）
    dim_city = spark.table("tms_dim.dim_region_full") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id"),
        col("name"),
        col("parent_id").alias("province_id")
    ).alias("city")

    # 5. 读取地区维度数据（省份级别）
    dim_province = spark.table("tms_dim.dim_region_full") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id"),
        col("name")
    ).alias("province")

    # 6. 执行多表关联
    result_df = deliver_detail.join(
        dim_org,
        col("detail.receiver_district_id") == col("organ.district_id"),
        "left"
    ).join(
        dim_district,
        col("organ.district_id") == col("district.id"),
        "left"
    ).join(
        dim_city,
        col("district.city_id") == col("city.id"),
        "left"
    ).join(
        dim_province,
        col("city.province_id") == col("province.id"),
        "left"
    ).groupBy(
        "org_id", "org_name",
        "city.id", "city.name",  # 城市ID和名称
        "province_id", "province.name",  # 省份ID和名称
        "detail.dt"  # 源数据分区
    ).agg(
        count("order_id").alias("order_count")
    ).select(
        col("org_id"),
        col("org_name"),
        col("id").alias("city_id"),
        col("city.name").alias("city_name"),
        col("province_id"),
        col("province.name").alias("province_name"),
        col("order_count"),
        col("dt")
    )

    # 添加目标分区字段
    final_df = result_df.withColumn("dt", lit(partition_date))

    print(f"[INFO] 数据处理完成，准备写入分区{partition_date}")
    final_df.show(5)  # 预览5条数据

    # 写入Hive
    write_to_hive(final_df, table_name, partition_date)

if __name__ == "__main__":
    target_table = 'dws_trans_org_deliver_suc_1d'  # 目标表名
    target_partition = '20250725'  # 目标分区字段值
    process_org_deliver_suc_stats(target_partition, target_table)