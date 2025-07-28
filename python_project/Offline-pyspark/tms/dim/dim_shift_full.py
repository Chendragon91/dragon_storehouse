from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("ShiftInfoETL") \
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
    jdbcDF.write \
        .mode('append') \
        .insertInto(f"tms_dim.{tableName}")

def execute_hive_insert(partition_date: str, tableName):
    """主处理逻辑：连接三张表并写入目标分区"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理班次信息，目标分区日期：{partition_date}")
    dt = "20250713"  # 源数据分区日期

    # 1. 读取ods_line_base_shift表数据（班次基础信息）
    shift_info = spark.table("tms.ods_line_base_shift") \
        .filter((col("dt") == dt) & (col("is_deleted") == "0")) \
        .select(
        col("id"),
        col("line_id"),
        col("start_time"),
        col("driver1_emp_id"),
        col("driver2_emp_id"),
        col("truck_id"),
        col("pair_shift_id")
    )

    # 2. 读取ods_line_base_info表数据（线路基础信息）
    line_info = spark.table("tms.ods_line_base_info") \
        .filter((col("dt") == dt) & (col("is_deleted") == "0")) \
        .select(
        col("id").alias("line_id"),  # 注意重命名用于join
        col("name").alias("line_name"),
        col("line_no"),
        col("line_level"),
        col("org_id"),
        col("transport_line_type_id"),
        col("start_org_id"),
        col("start_org_name"),
        col("end_org_id"),
        col("end_org_name"),
        col("pair_line_id"),
        col("distance"),
        col("cost"),
        col("estimated_time")
    )

    # 3. 读取ods_base_dic表数据（字典信息）
    dic_info = spark.table("tms.ods_base_dic") \
        .filter((col("dt") == dt) & (col("is_deleted") == "0")) \
        .select(
        col("id").alias("dic_id"),  # 注意重命名用于join
        col("name").alias("transport_line_type_name")
    )

    # 4. 三表关联操作
    # 首先关联班次表和线路表
    joined_df = shift_info.join(
        line_info,
        shift_info.line_id == line_info.line_id,
        "inner"
    )

    # 再关联字典表
    final_df = joined_df.join(
        dic_info,
        joined_df.transport_line_type_id == dic_info.dic_id,
        "inner"
    ).select(
        shift_info.id,
        shift_info.line_id,
        line_info.line_name,
        line_info.line_no,
        line_info.line_level,
        line_info.org_id,
        line_info.transport_line_type_id,
        dic_info.transport_line_type_name,
        line_info.start_org_id,
        line_info.start_org_name,
        line_info.end_org_id,
        line_info.end_org_name,
        line_info.pair_line_id,
        line_info.distance,
        line_info.cost,
        line_info.estimated_time,
        shift_info.start_time,
        shift_info.driver1_emp_id,
        shift_info.driver2_emp_id,
        shift_info.truck_id,
        shift_info.pair_shift_id
    )

    # 添加分区字段
    final_df_with_partition = final_df.withColumn("ds", lit(partition_date))

    print(f"[INFO] 数据处理完成，准备写入分区{partition_date}")
    final_df_with_partition.show(5)  # 预览5条数据

    # 写入Hive
    select_to_hive(final_df_with_partition, tableName, partition_date)

if __name__ == "__main__":
    table_name = 'dim_shift_full'  # 目标表名
    target_date = '20250725'  # 目标分区日期
    execute_hive_insert(target_date, table_name)