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
    INSERT OVERWRITE TABLE gmall_08.{tableName} PARTITION(dt='{partition_date}')
    SELECT
        a.activity_id,
        a.activity_name,
        a.status,
        DATE_FORMAT(a.start_time, 'yyyy-MM-dd') AS start_date,
        DATE_FORMAT(a.end_time, 'yyyy-MM-dd') AS end_date,
        COUNT(DISTINCT s.record_id) AS send_count,
        COUNT(DISTINCT r.redemption_id) AS redemption_count,
        ROUND(COUNT(DISTINCT r.redemption_id)/NULLIF(COUNT(DISTINCT s.record_id), 0), 4) AS redemption_rate,
        SUM(r.payment_amount) AS total_sales,
        NULL AS sales_increment,
        ROUND(AVG(
                      CASE
                          WHEN r.redemption_time IS NOT NULL
                              THEN (UNIX_TIMESTAMP(r.redemption_time) - UNIX_TIMESTAMP(s.send_time))/3600
                          ELSE NULL
                          END
                  ), 2) AS avg_redemption_time,
        MAX(CASE WHEN pr.rn = 1 THEN s.product_id ELSE NULL END) AS top_product_id,
        NULL AS top_product_name
    FROM dwd_cs_offer_activity_detail a
             LEFT JOIN dwd_cs_offer_send_detail s ON a.activity_id = s.activity_id AND s.dt = '{partition_date}'
             LEFT JOIN dwd_cs_offer_redemption_detail r ON s.record_id = r.record_id AND r.dt = '{partition_date}'
             LEFT JOIN (
        SELECT
            s.activity_id,
            s.product_id,
            ROW_NUMBER() OVER(PARTITION BY s.activity_id ORDER BY COUNT(DISTINCT r.redemption_id) DESC) AS rn
        FROM dwd_cs_offer_send_detail s
                 LEFT JOIN dwd_cs_offer_redemption_detail r ON s.record_id = r.record_id AND r.dt = '{partition_date}'
        WHERE s.dt = '{partition_date}'
        GROUP BY s.activity_id, s.product_id
    ) pr ON a.activity_id = pr.activity_id AND s.product_id = pr.product_id
    WHERE a.dt = '{partition_date}'
    GROUP BY
        a.activity_id,
        a.activity_name,
        a.status,
        a.start_time,
        a.end_time
    """

    try:
        spark.sql(insert_sql)
        print(f"[INFO] 数据写入完成，分区{partition_date}操作成功")
    except Exception as e:
        print(f"[ERROR] 执行失败: {str(e)}")
        raise

if __name__ == "__main__":
    table_name = 'ads_cs_offer_activity_effect'
    target_date = '20250731'

    try:
        execute_hive_insert(target_date, table_name)
    except Exception as e:
        print(f"[ERROR] 执行失败: {str(e)}")
