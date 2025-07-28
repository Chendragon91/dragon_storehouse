from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, count, sum, max, when, explode, array

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("TransShiftFinishETL") \
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

def process_shift_trans_finish_stats(partition_date: str, table_name: str):
    """处理班次转运完成统计信息并写入Hive"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理班次转运完成统计，目标分区日期：{partition_date}")
    source_dt = "20250713"  # 源数据分区日期

    # 1. 读取转运完成明细数据并展开recent_days
    trans_finish_detail = spark.table("tms_dwd.dwd_trans_trans_finish_inc") \
        .filter(col("dt") == source_dt) \
        .withColumn("recent_days", explode(array(lit(7), lit(30)))) \
        .alias("detail")

    # 2. 聚合统计
    aggregated = trans_finish_detail.groupBy(
        "recent_days",
        "shift_id",
        "line_id",
        "start_org_id",
        "start_org_name",
        "driver1_emp_id",
        "driver1_name",
        "driver2_emp_id",
        "driver2_name",
        "truck_id"
    ).agg(
        count("id").alias("trans_finish_count"),
        sum("actual_distance").alias("trans_finish_distance"),
        sum("finish_dur_sec").alias("trans_finish_dur_sec"),
        sum("order_num").alias("trans_finish_order_count"),
        sum(when(col("actual_end_time") > col("estimate_end_time"), 1).otherwise(0)).alias("trans_finish_delay_count")
    ).select(
        col("recent_days"),
        col("shift_id"),
        col("line_id"),
        col("start_org_id").alias("org_id"),
        col("start_org_name").alias("org_name"),
        col("driver1_emp_id"),
        col("driver1_name"),
        col("driver2_emp_id"),
        col("driver2_name"),
        col("truck_id"),
        col("trans_finish_count"),
        col("trans_finish_distance"),
        col("trans_finish_dur_sec"),
        col("trans_finish_order_count"),
        col("trans_finish_delay_count")
    ).alias("aggregated")

    # 3. 读取机构维度数据
    dim_org = spark.table("tms_dim.dim_organ_full") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id"),
        col("org_level"),
        col("region_id"),
        col("region_name")
    ).alias("first")

    # 4. 读取地区维度数据（父级）
    dim_parent = spark.table("tms_dim.dim_region_full") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id"),
        col("parent_id")
    ).alias("parent")

    # 5. 读取地区维度数据（城市）
    dim_city = spark.table("tms_dim.dim_region_full") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id"),
        col("name")
    ).alias("city")

    # 6. 读取班次维度数据
    dim_shift = spark.table("tms_dim.dim_shift_full") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id"),
        col("line_name")
    ).alias("for_line_name")

    # 7. 读取卡车维度数据
    dim_truck = spark.table("tms_dim.dim_truck_full") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id"),
        col("truck_model_type"),
        col("truck_model_type_name")
    ).alias("truck_info")

    # 8. 执行多表关联
    result_df = aggregated.join(
        dim_org,
        col("aggregated.org_id") == col("first.id"),
        "left"
    ).join(
        dim_parent,
        col("first.region_id") == col("parent.id"),
        "left"
    ).join(
        dim_city,
        col("parent.parent_id") == col("city.id"),
        "left"
    ).join(
        dim_shift,
        col("aggregated.shift_id") == col("for_line_name.id"),
        "left"
    ).join(
        dim_truck,
        col("aggregated.truck_id") == col("truck_info.id"),
        "left"
    ).select(
        col("aggregated.shift_id"),
        # 根据机构级别选择城市ID
        when(col("first.org_level") == 1, col("first.region_id"))
        .otherwise(col("city.id"))
        .alias("city_id"),
        # 根据机构级别选择城市名称
        when(col("first.org_level") == 1, col("first.region_name"))
        .otherwise(col("city.name"))
        .alias("city_name"),
        col("aggregated.org_id"),
        col("aggregated.org_name"),
        col("aggregated.line_id"),
        col("for_line_name.line_name"),
        col("aggregated.driver1_emp_id"),
        col("aggregated.driver1_name"),
        col("aggregated.driver2_emp_id"),
        col("aggregated.driver2_name"),
        col("truck_info.truck_model_type"),
        col("truck_info.truck_model_type_name"),
        col("aggregated.recent_days"),
        col("trans_finish_count"),
        col("trans_finish_distance"),
        col("trans_finish_dur_sec"),
        col("trans_finish_order_count"),
        col("trans_finish_delay_count")
    )

    # 添加目标分区字段
    final_df = result_df.withColumn("dt", lit(partition_date))

    print(f"[INFO] 数据处理完成，准备写入分区{partition_date}")
    final_df.show(5)  # 预览5条数据

    # 写入Hive
    write_to_hive(final_df, table_name, partition_date)

if __name__ == "__main__":
    target_table = 'dws_trans_shift_trans_finish_nd'  # 目标表名
    target_partition = '20250725'  # 目标分区字段值
    process_shift_trans_finish_stats(target_partition, target_table)