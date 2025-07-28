from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, count, sum

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("OrgTruckTransFinishETL") \
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
    df.drop('ds').write \
        .mode('append') \
        .insertInto(f"tms_dws.{table_name}")

def process_org_truck_trans_finish(partition_date: str, table_name: str):
    """处理机构卡车类别运输完成统计信息并写入Hive"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理机构卡车运输完成统计，目标分区日期：{partition_date}")
    source_dt = "20250713"  # 其他表的分区日期

    # 1. 读取运输完成事实表数据
    trans_finish_df = spark.table("tms_dwd.dwd_trans_trans_finish_inc") \
        .select(
        col("id"),
        col("start_org_id").alias("org_id"),
        col("start_org_name").alias("org_name"),
        col("truck_id"),
        col("actual_distance"),
        col("finish_dur_sec"),
        col("dt")
    ).alias("trans_finish")

    # 2. 读取卡车维度表数据（分区为20250713）
    truck_info_df = spark.table("tms_dim.dim_truck_full") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id"),
        col("truck_model_type"),
        col("truck_model_type_name")
    ).alias("truck_info")

    # 3. 关联两个表
    joined_df = trans_finish_df.join(
        truck_info_df,
        col("trans_finish.truck_id") == col("truck_info.id"),
        "left"
    )

    # 4. 按多维度分组聚合
    result_df = joined_df.groupBy(
        "org_id",
        "org_name",
        "truck_model_type",
        "truck_model_type_name",
        "dt"
    ).agg(
        count("trans_finish.id").alias("trans_finish_count"),
        sum("actual_distance").alias("trans_finish_distance"),
        sum("finish_dur_sec").alias("trans_finish_dur_sec")
    )

    # 5. 确保字段类型匹配Hive表定义
    final_df = result_df.select(
        col("org_id").cast("bigint"),
        col("org_name"),
        col("truck_model_type"),
        col("truck_model_type_name"),
        col("trans_finish_count").cast("bigint"),
        col("trans_finish_distance").cast("decimal(16, 2)"),
        col("trans_finish_dur_sec").cast("bigint"),
        col("dt")
    )

    # 6. 添加目标分区字段
    final_df = final_df.withColumn("ds", lit(partition_date))

    print("[INFO] 聚合结果预览：")
    final_df.show(5, truncate=False)

    # 7. 写入Hive
    write_to_hive(final_df, table_name, partition_date)

if __name__ == "__main__":
    target_table = 'dws_trans_org_truck_model_type_trans_finish_1d'
    target_partition = '20250725'  # 目标分区字段值
    process_org_truck_trans_finish(target_partition, target_table)