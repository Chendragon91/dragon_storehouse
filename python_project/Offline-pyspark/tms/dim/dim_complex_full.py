from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, collect_set, col
from pyspark.sql.types import StringType


# 1. 初始化SparkSession（全局单例模式）
def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("HiveETL") \
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
    # 使用insertInto方法写入已存在的分区表
    jdbcDF.write \
        .mode('append') \
        .insertInto(f"tms_dim.{tableName}")


# 2. 执行数据处理并插入Hive（算子实现）
def execute_hive_insert(partition_date: str, tableName):
    spark = get_spark_session()
    print(f"[INFO] 开始执行算子处理，分区日期：{partition_date}")

    # 1. 读取小区信息表
    complex_info = spark.table("tms.ods_base_complex") \
        .filter((col("dt") == "20250713") & (col("is_deleted") == "0")) \
        .select(
        col("id"),
        col("complex_name"),
        col("province_id"),
        col("city_id"),
        col("district_id"),
        col("district_name")
    )

    # 2. 读取区域字典表
    region_dict = spark.table("tms.ods_base_region_info") \
        .filter((col("dt") == "20250713") & (col("is_deleted") == "0")) \
        .select(
        col("id"),
        col("name")
    )

    # 3. 处理快递员数据（分组聚合）
    courier_data = spark.table("tms.ods_express_courier_complex") \
        .filter((col("dt") == "20250713") & (col("is_deleted") == "0")) \
        .groupBy(col("complex_id")) \
        .agg(
        collect_set(col("courier_emp_id").cast(StringType())).alias("courier_emp_ids")
    )

    # 4. 多表关联
    # 关联省份信息
    df_with_province = complex_info.join(
        region_dict.alias("province"),
        complex_info["province_id"] == col("province.id"),
        "inner"
    ).select(
        complex_info["id"],
        complex_info["complex_name"],
        complex_info["province_id"],
        col("province.name").alias("province_name"),
        complex_info["city_id"],
        complex_info["district_id"],
        complex_info["district_name"]
    )

    # 关联城市信息
    df_with_city = df_with_province.join(
        region_dict.alias("city"),
        df_with_province["city_id"] == col("city.id"),
        "inner"
    ).select(
        df_with_province["id"],
        df_with_province["complex_name"],
        df_with_province["province_id"],
        df_with_province["province_name"],
        df_with_province["city_id"],
        col("city.name").alias("city_name"),
        df_with_province["district_id"],
        df_with_province["district_name"]
    )

    # 关联快递员信息（左连接）
    final_df = df_with_city.join(
        courier_data,
        df_with_city["id"] == courier_data["complex_id"],
        "left"
    ).select(
        df_with_city["id"],
        df_with_city["complex_name"],
        col("courier_emp_ids"),
        df_with_city["province_id"],
        df_with_city["province_name"],
        df_with_city["city_id"],
        df_with_city["city_name"],
        df_with_city["district_id"],
        df_with_city["district_name"]
    )

    # 添加分区字段
    df_with_partition = final_df.withColumn("ds", lit(partition_date))

    print(f"[INFO] 算子处理完成，分区{partition_date}操作成功")
    df_with_partition.show()

    # 写入Hive
    select_to_hive(df_with_partition, tableName, partition_date)


# 3. 主函数（示例调用）
if __name__ == "__main__":
    table_name = 'dim_complex_full'
    # 设置目标分区日期
    target_date = '20250725'
    # 执行插入操作
    execute_hive_insert(target_date, table_name)