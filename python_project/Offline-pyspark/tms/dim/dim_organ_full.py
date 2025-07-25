from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StringType


# 初始化SparkSession（全局单例模式）
def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("OrganInfoETL") \
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


# 执行数据处理并插入Hive（算子实现）
def execute_hive_insert(partition_date: str, tableName):
    spark = get_spark_session()
    print(f"[INFO] 开始执行算子处理，分区日期：{partition_date}")
    dt = "20250713"  # 源数据分区日期

    # 对应子查询organ_info
    organ_info = spark.table("tms.ods_base_organ") \
        .filter((col("dt") == dt) & (col("is_deleted") == "0")) \
        .select(
        col("id"),
        col("org_name"),
        col("org_level"),
        col("region_id"),
        col("org_parent_id")
    )

    # 对应子查询region_info
    region_info = spark.table("tms.ods_base_region_info") \
        .filter((col("dt") == dt) & (col("is_deleted") == "0")) \
        .select(
        col("id"),
        col("name"),
        col("dict_code")
    )

    # 对应子查询org_for_parent
    org_for_parent = spark.table("tms.ods_base_organ") \
        .filter((col("dt") == dt) & (col("is_deleted") == "0")) \
        .select(
        col("id"),
        col("org_name")
    )

    # 第一步：关联organ_info和region_info（左连接）
    join_region = organ_info.join(
        region_info,
        organ_info["region_id"] == region_info["id"],
        "left"
    ).select(
        organ_info["id"],
        organ_info["org_name"],
        organ_info["org_level"],
        organ_info["region_id"],
        region_info["name"].alias("region_name"),
        region_info["dict_code"].alias("region_code"),
        organ_info["org_parent_id"]
    )

    # 第二步：关联父组织信息（左连接）
    final_df = join_region.join(
        org_for_parent,
        join_region["org_parent_id"] == org_for_parent["id"],
        "left"
    ).select(
        join_region["id"],
        join_region["org_name"],
        join_region["org_level"],
        join_region["region_id"],
        join_region["region_name"],
        join_region["region_code"],
        join_region["org_parent_id"],
        org_for_parent["org_name"].alias("org_parent_name")
    )

    # 添加分区字段
    df_with_partition = final_df.withColumn("ds", lit(partition_date))

    print(f"[INFO] 算子处理完成，分区{partition_date}操作成功")
    df_with_partition.show()

    # 写入Hive
    select_to_hive(df_with_partition, tableName, partition_date)


# 主函数（示例调用）
if __name__ == "__main__":
    table_name = 'dim_organ_full'  # 可根据实际表名调整
    # 设置目标分区日期
    target_date = '20250725'
    # 执行插入操作
    execute_hive_insert(target_date, table_name)