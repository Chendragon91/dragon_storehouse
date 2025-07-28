from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, date_format, lit, concat, substring, md5

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("TransportTaskETL") \
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
        .insertInto(f"tms_dwd.{tableName}")

def process_transport_task(partition_date: str, tableName):
    """处理运输任务信息并写入Hive"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理运输任务信息，目标分区日期：{partition_date}")
    source_dt = "20250713"  # 源数据分区日期

    # 读取运输任务数据
    transport_task = spark.table("tms.ods_transport_task") \
        .filter((col("dt") == source_dt) & (col("is_deleted") == "0") & col("actual_end_time").isNotNull()) \
        .select(
        col("id"),
        col("shift_id"),
        col("line_id"),
        col("start_org_id"),
        col("start_org_name"),
        col("end_org_id"),
        col("end_org_name"),
        col("order_num"),
        col("driver1_emp_id"),
        concat(substring(col("driver1_name"), 1, 1), lit("*")).alias("driver1_name"),
        col("driver2_emp_id"),
        concat(substring(col("driver2_name"), 1, 1), lit("*")).alias("driver2_name"),
        col("truck_id"),
        md5(col("truck_no")).alias("truck_no"),
        date_format(from_unixtime(col("actual_start_time")), "yyyy-MM-dd HH:mm:ss").alias("actual_start_time"),
        date_format(from_unixtime(col("actual_end_time")), "yyyy-MM-dd HH:mm:ss").alias("actual_end_time"),
        col("actual_distance"),
        ((col("actual_end_time").cast("bigint") - col("actual_start_time").cast("bigint")) / 1000).alias("finish_dur_sec"),
        date_format(from_unixtime(col("actual_end_time")), "yyyy-MM-dd").alias("dt")
    ).alias("info")

    # 读取班次维度数据
    dim_shift = spark.table("tms_dim.dim_shift_full") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id"),
        col("estimated_time")
    ).alias("dim_tb")

    # 关联查询
    result_df = transport_task.join(
        dim_shift,
        col("info.shift_id") == col("dim_tb.id"),
        "left"
    ).select(
        col("info.id"),
        col("info.shift_id"),
        col("info.line_id"),
        col("info.start_org_id"),
        col("info.start_org_name"),
        col("info.end_org_id"),
        col("info.end_org_name"),
        col("info.order_num"),
        col("info.driver1_emp_id"),
        col("info.driver1_name"),
        col("info.driver2_emp_id"),
        col("info.driver2_name"),
        col("info.truck_id"),
        col("info.truck_no"),
        col("info.actual_start_time"),
        col("info.actual_end_time"),
        col("dim_tb.estimated_time").alias("estimate_end_time"),
        col("info.actual_distance"),
        col("info.finish_dur_sec"),
        col("info.dt")
    )

    # 添加目标分区字段
    final_df = result_df.withColumn("ds", lit(partition_date))

    print(f"[INFO] 数据处理完成，准备写入分区{partition_date}")
    final_df.show(5)  # 预览5条数据

    # 写入Hive
    select_to_hive(final_df, tableName, partition_date)

if __name__ == "__main__":
    target_table = 'dwd_trans_trans_finish_inc'  # 目标表名
    target_partition = '20250725'  # 目标分区字段值
    process_transport_task(target_partition, target_table)