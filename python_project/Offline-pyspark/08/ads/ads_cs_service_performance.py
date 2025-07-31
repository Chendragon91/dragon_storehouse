from pyspark.sql import SparkSession

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

def execute_hive_insert(partition_date: str, tableName):
    spark = get_spark_session()

    print(f"[INFO] 开始执行SQL插入，分区日期：{partition_date}")

    # 直接使用INSERT OVERWRITE方式
    insert_sql = f"""
    INSERT OVERWRITE TABLE ads_cs_service_performance PARTITION(dt='{partition_date}')
SELECT
    p.cs_id,
    p.cs_name,
    p.dept_name,
    p.stat_date,
    p.send_count,
    p.redemption_count,
    p.redemption_rate,
    p.total_sales,
    p.rank,
    p.rank - prev.rank AS rank_change,
    p.avg_offer_amount,
    p.avg_redemption_time
FROM (
         SELECT
             cs_id,
             cs_name,
             dept_name,
             stat_date,
             send_count,
             redemption_count,
             redemption_rate,
             total_sales,
             RANK() OVER(PARTITION BY dept_name ORDER BY redemption_count DESC) AS rank,
             avg_offer_amount,
             avg_redemption_time
         FROM dws_cs_service_performance_wide
         WHERE dt = '{partition_date}' AND stat_date = DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM-dd')
     ) p
         LEFT JOIN (
    SELECT
        cs_id,
        stat_date,
        RANK() OVER(PARTITION BY dept_name ORDER BY redemption_count DESC) AS rank
    FROM dws_cs_service_performance_wide
    WHERE dt = DATE_FORMAT(DATE_SUB(TO_DATE('{partition_date}'), 1), 'yyyyMMdd')
      AND stat_date = DATE_FORMAT(DATE_SUB(CURRENT_DATE(), 1), 'yyyy-MM-dd')
) prev ON p.cs_id = prev.cs_id;
    """

    try:
        spark.sql(insert_sql)
        print(f"[INFO] 数据写入完成，分区{partition_date}操作成功")
    except Exception as e:
        print(f"[ERROR] 执行失败: {str(e)}")
        raise

if __name__ == "__main__":
    table_name = 'ads_cs_service_performance'
    target_date = '20250731'

    # 添加错误处理和重试机制
    try:
        execute_hive_insert(target_date, table_name)
    except Exception as e:
        print(f"[ERROR] 执行失败: {str(e)}")
        # 这里可以添加重试逻辑或通知机制
