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
    a.activity_id,
    a.activity_name,
    DATE_FORMAT(a.start_time, 'yyyy-MM-dd') AS start_date,
    DATE_FORMAT(a.end_time, 'yyyy-MM-dd') AS end_date,
    a.status,
    COUNT(DISTINCT s.record_id) AS total_send_count,
    COUNT(DISTINCT r.redemption_id) AS total_redemption_count,
    ROUND(COUNT(DISTINCT r.redemption_id) / NULLIF(COUNT(DISTINCT s.record_id), 0), 4) AS redemption_rate,
    SUM(r.payment_amount) AS total_sales,
    ROUND(AVG(s.offer_amount), 2) AS avg_offer_amount,
    MAX(s.offer_amount) AS max_offer_amount,
    MIN(s.offer_amount) AS min_offer_amount,
    COUNT(DISTINCT s.customer_id) AS total_customers,
    ROUND(AVG(
                  CASE
                      WHEN r.redemption_time IS NOT NULL
                          THEN (UNIX_TIMESTAMP(r.redemption_time) - UNIX_TIMESTAMP(s.send_time))/3600
                      ELSE NULL
                      END
              ), 2) AS avg_redemption_time,
    '{partition_date}' AS dt
FROM dwd_cs_offer_activity_detail a
         LEFT JOIN dwd_cs_offer_send_detail s ON a.activity_id = s.activity_id AND s.dt = '{partition_date}'
         LEFT JOIN dwd_cs_offer_redemption_detail r ON s.record_id = r.record_id AND r.dt = '{partition_date}'
WHERE a.dt = '{partition_date}'
GROUP BY
    a.activity_id, a.activity_name, a.start_time, a.end_time, a.status;
    """

    print(f"[INFO] 开始执行SQL插入，分区日期：{partition_date}")
    df = spark.sql(select_sql)

    df = df.na.fill(0)

    print(f"[INFO] 数据预览：")
    df.show(5)

    select_to_hive(df, tableName, partition_date)
    print(f"[INFO] 数据写入完成，分区{partition_date}操作成功")

if __name__ == "__main__":
    table_name = 'dws_cs_offer_activity_stats'
    target_date = '20250731'

    try:
        execute_hive_insert(target_date, table_name)
    except Exception as e:
        print(f"[ERROR] 执行失败: {str(e)}")
