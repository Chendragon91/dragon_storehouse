from pyspark.sql import SparkSession

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
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

    print(f"[INFO] 执行SQL查询，分区日期：{partition_date}")

    # 使用已经在系统中验证过的SQL逻辑
    insert_sql = f"""
    INSERT OVERWRITE TABLE gmall_08.{tableName} PARTITION(dt='{partition_date}')
    SELECT
        'day' AS time_type,
        DATE_FORMAT(s.send_time, 'yyyy-MM-dd') AS time_value,
        COUNT(DISTINCT s.record_id) AS send_count,
        COUNT(DISTINCT r.redemption_id) AS redemption_count,
        ROUND(COUNT(DISTINCT r.redemption_id) / NULLIF(COUNT(DISTINCT s.record_id), 0), 4) AS redemption_rate,
        ROUND(AVG(
                      CASE
                          WHEN r.redemption_time IS NOT NULL
                              THEN (UNIX_TIMESTAMP(r.redemption_time) - UNIX_TIMESTAMP(s.send_time))/3600
                          ELSE NULL
                          END
                  ), 2) AS avg_process_time,
        ROUND(AVG(s.offer_amount), 2) AS avg_offer_amount,
        CONCAT(
                LPAD(CAST(percentile_approx(HOUR(s.send_time), 0.5) AS INT), 2, '0'), ':00-',
                LPAD(CAST(percentile_approx(HOUR(s.send_time), 0.5) AS INT), 2, '0'), ':59'
            ) AS peak_hour,
        SUM(r.payment_amount) AS total_sales
    FROM dwd_cs_offer_send_detail s
             LEFT JOIN dwd_cs_offer_redemption_detail r ON s.record_id = r.record_id AND r.dt = '{partition_date}'
    WHERE s.dt = '{partition_date}'
    GROUP BY DATE_FORMAT(s.send_time, 'yyyy-MM-dd')

    UNION ALL

    SELECT
        'week' AS time_type,
        DATE_FORMAT(s.send_time, 'yyyy-ww') AS time_value,
        COUNT(DISTINCT s.record_id) AS send_count,
        COUNT(DISTINCT r.redemption_id) AS redemption_count,
        ROUND(COUNT(DISTINCT r.redemption_id) / NULLIF(COUNT(DISTINCT s.record_id), 0), 4) AS redemption_rate,
        ROUND(AVG(
                      CASE
                          WHEN r.redemption_time IS NOT NULL
                              THEN (UNIX_TIMESTAMP(r.redemption_time) - UNIX_TIMESTAMP(s.send_time))/3600
                          ELSE NULL
                          END
                  ), 2) AS avg_process_time,
        ROUND(AVG(s.offer_amount), 2) AS avg_offer_amount,
        CONCAT(
                LPAD(CAST(percentile_approx(HOUR(s.send_time), 0.5) AS INT), 2, '0'), ':00-',
                LPAD(CAST(percentile_approx(HOUR(s.send_time), 0.5) AS INT), 2, '0'), ':59'
            ) AS peak_hour,
        SUM(r.payment_amount) AS total_sales
    FROM dwd_cs_offer_send_detail s
             LEFT JOIN dwd_cs_offer_redemption_detail r ON s.record_id = r.record_id AND r.dt = '{partition_date}'
    WHERE s.dt = '{partition_date}'
    GROUP BY DATE_FORMAT(s.send_time, 'yyyy-ww')

    UNION ALL

    SELECT
        'month' AS time_type,
        DATE_FORMAT(s.send_time, 'yyyy-MM') AS time_value,
        COUNT(DISTINCT s.record_id) AS send_count,
        COUNT(DISTINCT r.redemption_id) AS redemption_count,
        ROUND(COUNT(DISTINCT r.redemption_id) / NULLIF(COUNT(DISTINCT s.record_id), 0), 4) AS redemption_rate,
        ROUND(AVG(
                      CASE
                          WHEN r.redemption_time IS NOT NULL
                              THEN (UNIX_TIMESTAMP(r.redemption_time) - UNIX_TIMESTAMP(s.send_time))/3600
                          ELSE NULL
                          END
                  ), 2) AS avg_process_time,
        ROUND(AVG(s.offer_amount), 2) AS avg_offer_amount,
        CONCAT(
                LPAD(CAST(percentile_approx(HOUR(s.send_time), 0.5) AS INT), 2, '0'), ':00-',
                LPAD(CAST(percentile_approx(HOUR(s.send_time), 0.5) AS INT), 2, '0'), ':59'
            ) AS peak_hour,
        SUM(r.payment_amount) AS total_sales
    FROM dwd_cs_offer_send_detail s
             LEFT JOIN dwd_cs_offer_redemption_detail r ON s.record_id = r.record_id AND r.dt = '{partition_date}'
    WHERE s.dt = '{partition_date}'
    GROUP BY DATE_FORMAT(s.send_time, 'yyyy-MM')
    """

    try:
        # 执行插入操作
        spark.sql(insert_sql)
        print(f"[INFO] 数据写入完成，分区{partition_date}操作成功")
    except Exception as e:
        print(f"[ERROR] 执行失败: {str(e)}")
        raise

if __name__ == "__main__":
    table_name = 'dws_cs_offer_time_analysis'
    target_date = '20250731'

    # 添加错误处理和重试机制
    try:
        execute_hive_insert(target_date, table_name)
    except Exception as e:
        print(f"[ERROR] 执行失败: {str(e)}")
        # 这里可以添加重试逻辑或通知机制
