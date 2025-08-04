from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType, DoubleType, IntegerType

def get_spark_session():

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

    spark = get_spark_session()

    try:
        spark.sql(f"ALTER TABLE gmall_08.{tableName} DROP IF EXISTS PARTITION (dt='{partition_date}')")
    except:
        pass

    jdbcDF.write \
        .mode('overwrite') \
        .insertInto(f"gmall_08.{tableName}")

def execute_hive_insert(partition_date: str, tableName):
    spark = get_spark_session()

    select_sql = f"""
SELECT
    s.record_id,
    s.activity_id,
    s.product_id,
    s.sku_id,
    s.customer_id,
    s.cs_id,
    s.offer_amount,
    s.valid_hours,
    s.send_time,
    s.expire_time,
    s.status,
    s.remark,
    a.activity_name,
    a.offer_type,
    DATE_FORMAT(s.send_time, 'yyyy-MM-dd') AS date_str,
    DATE_FORMAT(s.send_time, 'HH') AS hour_str,
    CASE WHEN s.status = 2 THEN 1 ELSE 0 END AS is_used,
    CASE WHEN s.status = 3 OR (s.expire_time IS NOT NULL AND s.expire_time < CURRENT_TIMESTAMP) THEN 1 ELSE 0 END AS is_expired,
    CASE
        WHEN s.expire_time IS NULL THEN NULL
        WHEN s.expire_time < CURRENT_TIMESTAMP THEN 0
        ELSE ROUND((UNIX_TIMESTAMP(s.expire_time) - UNIX_TIMESTAMP(CURRENT_TIMESTAMP))/3600, 2)
        END AS remaining_time,
    '{partition_date}' AS dt
FROM ods_cs_offer_send_records s
         LEFT JOIN ods_cs_special_offer_activity a ON s.activity_id = a.activity_id AND a.dt = '{partition_date}'
WHERE s.dt = '{partition_date}';
    """

    print(f"[INFO] 开始执行SQL插入，分区日期：{partition_date}")
    df = spark.sql(select_sql)

    df = df.na.fill(0)

    print(f"[INFO] 数据预览：")
    df.show(5)

    select_to_hive(df, tableName, partition_date)
    print(f"[INFO] 数据写入完成，分区{partition_date}操作成功")

if __name__ == "__main__":
    table_name = 'dwd_cs_offer_send_detail'
    target_date = '20250731'

    try:
        execute_hive_insert(target_date, table_name)
    except Exception as e:
        print(f"[ERROR] 执行失败: {str(e)}")
