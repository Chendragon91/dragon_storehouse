from pyspark.sql import SparkSession

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

def execute_hive_insert(partition_date: str, tableName):
    spark = get_spark_session()

    print(f"[INFO] 开始执行SQL插入，分区日期：{partition_date}")

    insert_sql = f"""
    INSERT OVERWRITE TABLE ads_cs_offer_overview PARTITION(dt='{partition_date}')
SELECT
    CURRENT_DATE() AS stat_date,
    '1' AS time_range,
    COUNT(record_id) AS total_send_count,
    SUM(is_used) AS total_redemption_count,
    ROUND(SUM(is_used)/NULLIF(COUNT(record_id), 0), 4) AS overall_redemption_rate,
    NULL AS total_sales,
    NULL AS sales_increment_rate,
    ROUND(AVG(offer_amount), 2) AS avg_offer_amount,
    COUNT(DISTINCT cs_id) AS active_cs_count,
    COUNT(DISTINCT customer_id) AS active_customer_count
FROM dwd_cs_offer_send_detail
WHERE dt = '{partition_date}' AND date_str = DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM-dd')

UNION ALL

SELECT
    CURRENT_DATE() AS stat_date,
    '7' AS time_range,
    COUNT(record_id) AS total_send_count,
    SUM(is_used) AS total_redemption_count,
    ROUND(SUM(is_used)/NULLIF(COUNT(record_id), 0), 4) AS overall_redemption_rate,
    NULL AS total_sales,
    NULL AS sales_increment_rate,
    ROUND(AVG(offer_amount), 2) AS avg_offer_amount,
    COUNT(DISTINCT cs_id) AS active_cs_count,
    COUNT(DISTINCT customer_id) AS active_customer_count
FROM dwd_cs_offer_send_detail
WHERE dt = '{partition_date}' AND date_str BETWEEN DATE_SUB(CURRENT_DATE(), 6) AND CURRENT_DATE()

UNION ALL

SELECT
    CURRENT_DATE() AS stat_date,
    '30' AS time_range,
    COUNT(record_id) AS total_send_count,
    SUM(is_used) AS total_redemption_count,
    ROUND(SUM(is_used)/NULLIF(COUNT(record_id), 0), 4) AS overall_redemption_rate,
    NULL AS total_sales,
    NULL AS sales_increment_rate,
    ROUND(AVG(offer_amount), 2) AS avg_offer_amount,
    COUNT(DISTINCT cs_id) AS active_cs_count,
    COUNT(DISTINCT customer_id) AS active_customer_count
FROM dwd_cs_offer_send_detail
WHERE dt = '{partition_date}' AND date_str BETWEEN DATE_SUB(CURRENT_DATE(), 29) AND CURRENT_DATE();
    """

    try:
        spark.sql(insert_sql)
        print(f"[INFO] 数据写入完成，分区{partition_date}操作成功")
    except Exception as e:
        print(f"[ERROR] 执行失败: {str(e)}")
        raise

if __name__ == "__main__":
    table_name = 'ads_cs_offer_overview'
    target_date = '20250731'

    try:
        execute_hive_insert(target_date, table_name)
    except Exception as e:
        print(f"[ERROR] 执行失败: {str(e)}")
