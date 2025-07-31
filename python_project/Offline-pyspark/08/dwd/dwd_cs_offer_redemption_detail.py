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
    redemption_id,
    record_id,
    order_id,
    redemption_time,
    actual_offer_amount,
    payment_amount,
    DATE_FORMAT(redemption_time, 'yyyy-MM-dd') AS date_str,
    DATE_FORMAT(redemption_time, 'yyyy-MM-dd') AS redemption_date,
    DATE_FORMAT(redemption_time, 'HH') AS redemption_hour,
    ROUND(actual_offer_amount / NULLIF(payment_amount, 0), 4) AS offer_rate,
    (payment_amount + actual_offer_amount) AS original_amount,
    '{partition_date}' AS dt  -- 添加分区列dt
FROM ods_cs_offer_redemption_records
WHERE dt = '{partition_date}'  -- 修复变量引用
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
    table_name = 'dwd_cs_offer_redemption_detail'
    target_date = '20250731'

    # 添加错误处理和重试机制
    try:
        execute_hive_insert(target_date, table_name)
    except Exception as e:
        print(f"[ERROR] 执行失败: {str(e)}")
        # 这里可以添加重试逻辑或通知机制
