from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("DriverInfoETL") \
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
    print(f"[INFO] 开始处理司机信息，目标分区日期：{partition_date}")
    dt = "20250713"  # 源数据分区日期

    # 1. 读取ods_truck_driver表数据（司机基础信息）
    driver_info = spark.table("tms.ods_truck_driver") \
        .filter((col("dt") == dt) & (col("is_deleted") == "0")) \
        .select(
        col("id"),
        col("emp_id"),
        col("org_id"),
        col("team_id"),
        col("license_type"),
        col("init_license_date"),
        col("expire_date"),
        col("license_no"),
        col("is_enabled")
    )

    # 2. 读取ods_base_organ表数据（组织信息）
    organ_info = spark.table("tms.ods_base_organ") \
        .filter((col("dt") == dt) & (col("is_deleted") == "0")) \
        .select(
        col("id").alias("organ_id"),  # 重命名用于join
        col("org_name")
    )

    # 3. 读取ods_truck_team表数据（车队信息）
    team_info = spark.table("tms.ods_truck_team") \
        .filter((col("dt") == dt) & (col("is_deleted") == "0")) \
        .select(
        col("id").alias("team_info_id"),  # 重命名用于join
        col("name").alias("team_name")
    )

    # 4. 三表关联操作
    # 首先关联司机表和组织表
    joined_df = driver_info.join(
        organ_info,
        driver_info.org_id == organ_info.organ_id,
        "inner"
    )

    # 再关联车队表
    final_df = joined_df.join(
        team_info,
        joined_df.team_id == team_info.team_info_id,
        "inner"
    ).select(
        driver_info.id,
        driver_info.emp_id,
        driver_info.org_id,
        organ_info.org_name,
        driver_info.team_id,
        team_info.team_name,
        driver_info.license_type,
        driver_info.init_license_date,
        driver_info.expire_date,
        driver_info.license_no,
        driver_info.is_enabled
    )

    # 添加分区字段
    final_df_with_partition = final_df.withColumn("ds", lit(partition_date))

    print(f"[INFO] 数据处理完成，准备写入分区{partition_date}")
    final_df_with_partition.show(5)  # 预览5条数据

    # 写入Hive
    select_to_hive(final_df_with_partition, tableName, partition_date)

if __name__ == "__main__":
    table_name = 'dim_truck_driver_full'  # 目标表名
    target_date = '20250725'  # 目标分区日期
    execute_hive_insert(target_date, table_name)