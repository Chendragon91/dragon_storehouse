from pyspark.sql import SparkSession

def get_spark_session():
    spark = SparkSession.builder \
        .appName("HiveETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.parquet.binaryAsString", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
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
    INSERT OVERWRITE TABLE gmall_08.{tableName} PARTITION(dt='{partition_date}')
    SELECT
        p.cs_id,
        p.cs_name,
        p.dept_name,
        p.stat_date,
        p.stat_week,
        p.stat_month,
        p.send_count,
        p.redemption_count,
        p.redemption_rate,
        p.total_sales,
        RANK() OVER(PARTITION BY p.dept_name, p.stat_date ORDER BY p.redemption_count DESC) AS rank_in_dept,
        p.avg_offer_amount,
        p.avg_redemption_time
    FROM (
             SELECT
                 cs.cs_id,
                 cs.cs_name,
                 cs.department AS dept_name,
                 DATE_FORMAT(s.send_time, 'yyyy-MM-dd') AS stat_date,
                 DATE_FORMAT(s.send_time, 'yyyy-ww') AS stat_week,
                 DATE_FORMAT(s.send_time, 'yyyy-MM') AS stat_month,
                 COUNT(s.record_id) AS send_count,
                 SUM(s.is_used) AS redemption_count,
                 ROUND(SUM(s.is_used)/NULLIF(COUNT(s.record_id), 0), 4) AS redemption_rate,
                 COALESCE(SUM(r.payment_amount), 0.0) AS total_sales,
                 ROUND(AVG(s.offer_amount), 2) AS avg_offer_amount,
                 ROUND(AVG(
                               CASE
                                   WHEN r.redemption_time IS NOT NULL
                                       THEN (UNIX_TIMESTAMP(r.redemption_time) - UNIX_TIMESTAMP(s.send_time))/3600
                                   ELSE NULL
                                   END
                           ), 2) AS avg_redemption_time
             FROM dwd_cs_offer_send_detail s
                      LEFT JOIN dwd_cs_offer_redemption_detail r ON s.record_id = r.record_id AND r.dt = '{partition_date}'
                      JOIN ods_customer_service_info cs ON s.cs_id = cs.cs_id AND cs.dt = '{partition_date}'
             WHERE s.dt = '{partition_date}'
             GROUP BY
                 cs.cs_id, cs.cs_name, cs.department,
                 DATE_FORMAT(s.send_time, 'yyyy-MM-dd'),
                 DATE_FORMAT(s.send_time, 'yyyy-ww'),
                 DATE_FORMAT(s.send_time, 'yyyy-MM')
         ) p
    """

    try:
        spark.sql(insert_sql)
        print(f"[INFO] 数据写入完成，分区{partition_date}操作成功")
    except Exception as e:
        print(f"[ERROR] 执行失败: {str(e)}")
        raise

if __name__ == "__main__":
    table_name = 'dws_cs_service_performance_wide'
    target_date = '20250731'

    try:
        execute_hive_insert(target_date, table_name)
    except Exception as e:
        print(f"[ERROR] 执行失败: {str(e)}")
