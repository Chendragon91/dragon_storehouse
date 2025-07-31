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
    p.product_id,
    -- 如果没有单独的商品名称表，可以使用固定值或留空
    '商品-' || CAST(p.product_id AS STRING) AS product_name,
    DATE_FORMAT(s.send_time, 'yyyy-MM-dd') AS stat_date,
    COUNT(DISTINCT s.record_id) AS send_count,
    COUNT(DISTINCT r.redemption_id) AS redemption_count,
    ROUND(COUNT(DISTINCT r.redemption_id) / NULLIF(COUNT(DISTINCT s.record_id), 0), 4) AS redemption_rate,
    SUM(r.payment_amount) AS total_sales,
    ROUND(AVG(
                  CASE
                      WHEN r.payment_amount > 0
                          THEN r.actual_offer_amount / r.payment_amount
                      ELSE NULL
                      END
              ), 4) AS avg_offer_rate,
    MAX(s.offer_amount) AS max_offer_amount,
    MIN(s.offer_amount) AS min_offer_amount,
    ROUND(AVG(
                  CASE
                      WHEN r.redemption_time IS NOT NULL
                          THEN (UNIX_TIMESTAMP(r.redemption_time) - UNIX_TIMESTAMP(s.send_time))/3600
                      ELSE NULL
                      END
              ), 2) AS avg_redemption_time,
    '{partition_date}' AS dt  -- 添加分区列
FROM dwd_cs_offer_send_detail s
         LEFT JOIN dwd_cs_offer_redemption_detail r ON s.record_id = r.record_id AND r.dt = '{partition_date}'
         LEFT JOIN ods_cs_special_offer_products p ON s.product_id = p.product_id AND p.dt = '{partition_date}'
WHERE s.dt = '{partition_date}'
GROUP BY
    p.product_id,
    -- 与product_name表达式保持一致
    '商品-' || CAST(p.product_id AS STRING),
    DATE_FORMAT(s.send_time, 'yyyy-MM-dd');
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
    table_name = 'dws_cs_offer_product_stats'
    target_date = '20250731'

    # 添加错误处理和重试机制
    try:
        execute_hive_insert(target_date, table_name)
    except Exception as e:
        print(f"[ERROR] 执行失败: {str(e)}")
        # 这里可以添加重试逻辑或通知机制
