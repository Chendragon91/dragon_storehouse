from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, md5

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("TruckInfoETL") \
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
    """主处理逻辑：连接多张表并写入目标分区"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理卡车信息，目标分区日期：{partition_date}")
    dt = "20250713"  # 源数据分区日期

    # 1. 读取ods_truck_info表数据（卡车基础信息）
    truck_info = spark.table("tms.ods_truck_info") \
        .filter((col("dt") == dt) & (col("is_deleted") == "0")) \
        .select(
        col("id"),
        col("team_id"),
        md5(col("truck_no")).alias("truck_no"),  # 使用md5加密车牌号
        col("truck_model_id"),
        col("device_gps_id"),
        col("engine_no"),
        col("license_registration_date"),
        col("license_last_check_date"),
        col("license_expire_date"),
        col("is_enabled")
    )

    # 2. 读取ods_truck_team表数据（车队信息）
    team_info = spark.table("tms.ods_truck_team") \
        .filter((col("dt") == dt) & (col("is_deleted") == "0")) \
        .select(
        col("id").alias("team_info_id"),  # 重命名用于join
        col("name").alias("team_name"),
        col("team_no"),
        col("org_id"),
        col("manager_emp_id")
    )

    # 3. 读取ods_truck_model表数据（车型信息）
    model_info = spark.table("tms.ods_truck_model") \
        .filter((col("dt") == dt) & (col("is_deleted") == "0")) \
        .select(
        col("id").alias("model_id"),  # 重命名用于join
        col("model_name"),
        col("model_type"),
        col("model_no"),
        col("brand"),
        col("truck_weight"),
        col("load_weight"),
        col("total_weight"),
        col("eev"),
        col("boxcar_len"),
        col("boxcar_wd"),
        col("boxcar_hg"),
        col("max_speed"),
        col("oil_vol")
    )

    # 4. 读取ods_base_organ表数据（组织信息）
    organ_info = spark.table("tms.ods_base_organ") \
        .filter((col("dt") == dt) & (col("is_deleted") == "0")) \
        .select(
        col("id").alias("organ_info_id"),  # 重命名用于join
        col("org_name")
    )

    # 5. 读取ods_base_dic表数据（字典信息-车型类型）
    dic_for_type = spark.table("tms.ods_base_dic") \
        .filter((col("dt") == dt) & (col("is_deleted") == "0")) \
        .select(
        col("id").alias("dic_type_id"),  # 重命名用于join
        col("name").alias("truck_model_type_name")
    )

    # 6. 读取ods_base_dic表数据（字典信息-品牌）
    dic_for_brand = spark.table("tms.ods_base_dic") \
        .filter((col("dt") == dt) & (col("is_deleted") == "0")) \
        .select(
        col("id").alias("dic_brand_id"),  # 重命名用于join
        col("name").alias("truck_brand_name")
    )

    # 7. 多表关联操作
    # 首先关联卡车表和车队表
    joined_step1 = truck_info.join(
        team_info,
        truck_info.team_id == team_info.team_info_id,
        "inner"
    )

    # 关联车型表
    joined_step2 = joined_step1.join(
        model_info,
        truck_info.truck_model_id == model_info.model_id,
        "inner"
    )

    # 关联组织表
    joined_step3 = joined_step2.join(
        organ_info,
        joined_step2.org_id == organ_info.organ_info_id,
        "inner"
    )

    # 关联车型类型字典表
    joined_step4 = joined_step3.join(
        dic_for_type,
        joined_step3.model_type == dic_for_type.dic_type_id,
        "inner"
    )

    # 关联品牌字典表
    final_df = joined_step4.join(
        dic_for_brand,
        joined_step4.brand == dic_for_brand.dic_brand_id,
        "inner"
    ).select(
        truck_info.id,
        truck_info.team_id,
        team_info.team_name,
        team_info.team_no,
        team_info.org_id,
        organ_info.org_name,
        team_info.manager_emp_id,
        truck_info.truck_no,
        truck_info.truck_model_id,
        model_info.model_name.alias("truck_model_name"),
        model_info.model_type.alias("truck_model_type"),
        dic_for_type.truck_model_type_name,
        model_info.model_no.alias("truck_model_no"),
        model_info.brand.alias("truck_brand"),
        dic_for_brand.truck_brand_name,
        model_info.truck_weight,
        model_info.load_weight,
        model_info.total_weight,
        model_info.eev,
        model_info.boxcar_len,
        model_info.boxcar_wd,
        model_info.boxcar_hg,
        model_info.max_speed,
        model_info.oil_vol,
        truck_info.device_gps_id,
        truck_info.engine_no,
        truck_info.license_registration_date,
        truck_info.license_last_check_date,
        truck_info.license_expire_date,
        truck_info.is_enabled
    )

    # 添加分区字段
    final_df_with_partition = final_df.withColumn("ds", lit(partition_date))

    print(f"[INFO] 数据处理完成，准备写入分区{partition_date}")
    final_df_with_partition.show(5)  # 预览5条数据

    # 写入Hive
    select_to_hive(final_df_with_partition, tableName, partition_date)

if __name__ == "__main__":
    table_name = 'dim_truck_full'  # 目标表名
    target_date = '20250725'  # 目标分区日期
    execute_hive_insert(target_date, table_name)