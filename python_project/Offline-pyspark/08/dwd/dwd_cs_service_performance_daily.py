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
    COALESCE(s.cs_id, r.cs_id) AS cs_id,
    COALESCE(s.stat_date, r.stat_date) AS stat_date,
    COALESCE(s.send_count, 0) AS send_count,
    COALESCE(r.redemption_count, 0) AS redemption_count,
    COALESCE(s.total_offer_amount, 0) AS total_offer_amount,
    COALESCE(r.total_sales, 0) AS total_sales,
    CASE
        WHEN COALESCE(s.send_count, 0) > 0
            THEN ROUND(COALESCE(s.total_offer_amount, 0) / s.send_count, 2)
        ELSE 0
        END AS avg_offer_amount,
    CASE
        WHEN COALESCE(r.redemption_count, 0) > 0
            THEN ROUND(COALESCE(r.total_sales, 0) / r.redemption_count, 2)
        ELSE 0
        END AS avg_order_amount,
    CASE
        WHEN COALESCE(s.send_count, 0) > 0
            THEN ROUND(COALESCE(r.redemption_count, 0) / s.send_count, 4)
        ELSE 0
        END AS redemption_rate,
    '{partition_date}' AS dt  -- 添加分区列
FROM (
         SELECT
             cs_id,
             DATE_FORMAT(send_time, 'yyyy-MM-dd') AS stat_date,
             COUNT(*) AS send_count,
             SUM(offer_amount) AS total_offer_amount
         FROM ods_cs_offer_send_records
         WHERE dt = '{partition_date}'  -- 修复变量引用
         GROUP BY cs_id, DATE_FORMAT(send_time, 'yyyy-MM-dd')
     ) s
         FULL OUTER JOIN (
    SELECT
        s.cs_id,
        DATE_FORMAT(r.redemption_time, 'yyyy-MM-dd') AS stat_date,
        COUNT(r.redemption_id) AS redemption_count,
        SUM(r.payment_amount) AS total_sales
    FROM ods_cs_offer_redemption_records r
             JOIN ods_cs_offer_send_records s ON r.record_id = s.record_id AND s.dt = '{partition_date}'  -- 修复变量引用
    WHERE r.dt = '{partition_date}'  -- 修复变量引用
    GROUP BY s.cs_id, DATE_FORMAT(r.redemption_time, 'yyyy-MM-dd')
) r ON s.cs_id = r.cs_id AND s.stat_date = r.stat_date;
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
    table_name = 'dwd_cs_service_performance_daily'
    target_date = '20250731'

    # 添加错误处理和重试机制
    try:
        execute_hive_insert(target_date, table_name)
    except Exception as e:
        print(f"[ERROR] 执行失败: {str(e)}")
        # 这里可以添加重试逻辑或通知机制
