from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_utc_timestamp, date_format, lit, to_timestamp

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("OrderOrgBoundSortETL") \
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

def process_order_org_sort(partition_date: str, tableName):
    """处理订单组织分拣信息并写入Hive"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理订单组织分拣信息，目标分区日期：{partition_date}")
    source_dt = "20250713"  # 源数据分区日期

    # 读取源表数据并转换（修复时间转换逻辑）
    df = spark.table("tms.ods_order_org_bound") \
        .filter((col("dt") == source_dt) & (col("sort_time").isNotNull())) \
        .select(
        col("id"),
        col("order_id"),
        col("org_id"),
        # 修复：使用to_timestamp转换为时间戳，再处理UTC转换
        date_format(
            from_utc_timestamp(
                to_timestamp(col("sort_time").cast("string"), "yyyy-MM-dd HH:mm:ss"),  # 先转为字符串再解析为时间戳
                "UTC"
            ),
            "yyyy-MM-dd HH:mm:ss"
        ).alias("sort_time"),
        col("sorter_emp_id"),
        # 提取日期部分作为dt字段
        date_format(
            from_utc_timestamp(
                to_timestamp(col("sort_time").cast("string"), "yyyy-MM-dd HH:mm:ss"),
                "UTC"
            ),
            "yyyy-MM-dd"
        ).alias("dt")
    )

    # 添加目标分区字段
    final_df = df.withColumn("ds", lit(partition_date))

    print(f"[INFO] 数据处理完成，准备写入分区{partition_date}")
    final_df.show(5)  # 预览5条数据

    # 写入Hive
    select_to_hive(final_df, tableName, partition_date)

if __name__ == "__main__":
    target_table = 'dwd_bound_sort_inc'  # 目标表名（根据实际情况修改）
    target_partition = '20250725'  # 目标分区字段值
    process_order_org_sort(target_partition, target_table)