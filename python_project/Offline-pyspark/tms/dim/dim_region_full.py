from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StringType


# 初始化SparkSession（全局单例模式）
def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("RegionInfoETL") \
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

    # 对应SQL查询：从ods_base_region_info筛选数据
    region_info = spark.table("tms.ods_base_region_info") \
        .filter((col("dt") == dt) & (col("is_deleted") == "0")) \
        .select(
        col("id"),
        col("parent_id"),
        col("name"),
        col("dict_code"),
        col("short_name")
    )

    # 添加分区字段
    df_with_partition = region_info.withColumn("ds", lit(partition_date))

    print(f"[INFO] 算子处理完成，分区{partition_date}操作成功")
    df_with_partition.show()

    # 写入Hive
    select_to_hive(df_with_partition, tableName, partition_date)


# 主函数（示例调用）
if __name__ == "__main__":
    table_name = 'dim_region_full'  # 可根据实际表名调整
    # 设置目标分区日期
    target_date = '20250725'
    # 执行插入操作
    execute_hive_insert(target_date, table_name)