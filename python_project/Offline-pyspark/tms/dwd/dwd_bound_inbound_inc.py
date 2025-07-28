from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, from_unixtime, date_format

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("OrderOrgBoundETL") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark

def select_to_hive(jdbcDF, tableName, partition_date):
    """将DataFrame写入Hive分区表（通过partitionBy指定分区）"""
    # 写入前先删除临时分区字段ds，只保留表中存在的列
    jdbcDF.drop("ds").write \
        .mode('append') \
        .insertInto(f"tms_dwd.{tableName}")

def process_order_org_bound(partition_date: str, tableName):
    """处理订单组织边界数据并写入Hive"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理订单组织边界数据，目标分区日期：{partition_date}")
    source_dt = "20250713"  # 源数据分区日期

    # 读取源表数据并转换（不包含ds字段）
    df = spark.table("tms.ods_order_org_bound") \
        .filter(col("dt") == source_dt) \
        .select(
        col("id"),
        col("order_id"),
        col("org_id"),
        # 转换inbound_time为时间字符串
        date_format(
            from_unixtime(col("inbound_time"), "yyyy-MM-dd HH:mm:ss"),
            "yyyy-MM-dd HH:mm:ss"
        ).alias("inbound_time"),
        col("inbound_emp_id"),
        # 提取日期部分作为dt字段
        date_format(
            from_unixtime(col("inbound_time"), "yyyy-MM-dd HH:mm:ss"),
            "yyyy-MM-dd"
        ).alias("dt")
    )

    # 添加分区字段ds用于写入（仅临时用于partitionBy）
    df_with_partition = df.withColumn("ds", lit(partition_date))

    print(f"[INFO] 数据处理完成，准备写入分区{partition_date}")
    df_with_partition.show(5)  # 预览数据

    # 写入Hive（通过partitionBy指定ds分区）
    select_to_hive(df_with_partition, tableName, partition_date)

if __name__ == "__main__":
    target_table = 'dwd_bound_inbound_inc'
    target_partition = '20250725'
    process_order_org_bound(target_partition, target_table)