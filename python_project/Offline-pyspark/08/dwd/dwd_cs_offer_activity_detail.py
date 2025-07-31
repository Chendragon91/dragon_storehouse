from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType, DoubleType, IntegerType

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("HiveETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.parquet.binaryAsString", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE gmall_08")
    return spark

def select_to_hive(jdbcDF, tableName, partition_date):
    """写入Hive分区表"""
    spark = get_spark_session()

    # 删除指定分区（如果存在）
    try:
        spark.sql(f"ALTER TABLE gmall_08.{tableName} DROP IF EXISTS PARTITION (dt='{partition_date}')")
    except:
        pass  # 如果分区不存在，忽略错误

    # 使用 insertInto 而不是 saveAsTable 来避免格式冲突
    jdbcDF.write \
        .mode('overwrite') \
        .insertInto(f"gmall_08.{tableName}")

def execute_hive_insert(partition_date: str, tableName):
    spark = get_spark_session()

    # 构建SQL查询，添加 dt 分区列
    select_sql = f"""
SELECT
    activity_id,
    activity_name,
    activity_level,
    offer_type,
    start_time,
    end_time,
    max_custom_amount,
    status,
    shop_id,
    create_time,
    update_time,
    ROUND((UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time))/3600, 2) AS activity_duration,
    CASE WHEN status = 1 AND end_time > CURRENT_TIMESTAMP THEN 1 ELSE 0 END AS is_valid,
    CASE WHEN activity_name LIKE '%测试%' THEN 1 ELSE 0 END AS is_test_data,
    '{partition_date}' as dt  -- 添加分区列
FROM ods_cs_special_offer_activity
WHERE dt = '{partition_date}'
    """

    print(f"[INFO] 开始执行SQL插入，分区日期：{partition_date}")
    df = spark.sql(select_sql)

    # 处理可能的空值
    df = df.na.fill(0)

    print(f"[INFO] 数据预览：")
    df.show(5)

    # 写入Hive
    select_to_hive(df, tableName, partition_date)
    print(f"[INFO] 数据写入完成，分区{partition_date}操作成功")

if __name__ == "__main__":
    table_name = 'dwd_cs_offer_activity_detail'
    target_date = '20250731'

    # 添加错误处理和重试机制
    try:
        execute_hive_insert(target_date, table_name)
    except Exception as e:
        print(f"[ERROR] 执行失败: {str(e)}")
        # 这里可以添加重试逻辑或通知机制
