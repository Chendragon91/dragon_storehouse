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
    INSERT OVERWRITE TABLE ads_cs_offer_send_details PARTITION(dt='{partition_date}')
SELECT
    s.record_id,
    s.send_time,
    cs.cs_name,
    s.customer_id,
    -- 修改为从活动商品表获取产品信息
    CASE
        WHEN op.product_id IS NOT NULL THEN CONCAT('活动商品-', op.product_id)
        ELSE '未知商品'
        END AS product_name,
    -- SKU名称暂设为NULL，因为没有相关表
    NULL AS sku_name,
    s.offer_amount,
    CASE s.status
        WHEN 1 THEN '未使用'
        WHEN 2 THEN '已使用'
        WHEN 3 THEN '已过期'
        ELSE '未知'
        END AS status,
    r.redemption_time,
    r.payment_amount AS order_amount,
    a.activity_name,
    s.expire_time,
    CONCAT(
            CASE
                WHEN s.remaining_time IS NULL THEN '无限制'
                WHEN s.remaining_time = 0 THEN '已过期'
                ELSE CAST(s.remaining_time AS STRING)
                END,
            '小时'
        ) AS remaining_time
FROM dwd_cs_offer_send_detail s
         LEFT JOIN dwd_cs_offer_redemption_detail r ON s.record_id = r.record_id AND r.dt = '{partition_date}'
         LEFT JOIN ods_customer_service_info cs ON s.cs_id = cs.cs_id AND cs.dt = '{partition_date}'
         LEFT JOIN ods_cs_special_offer_activity a ON s.activity_id = a.activity_id AND a.dt = '{partition_date}'
-- 修改为使用活动商品表
         LEFT JOIN ods_cs_special_offer_products op ON s.product_id = op.product_id AND op.dt = '{partition_date}'
WHERE s.dt = '{partition_date}';
    """

    try:
        spark.sql(insert_sql)
        print(f"[INFO] 数据写入完成，分区{partition_date}操作成功")
    except Exception as e:
        print(f"[ERROR] 执行失败: {str(e)}")
        raise

if __name__ == "__main__":
    table_name = 'ads_cs_offer_send_details'
    target_date = '20250731'

    try:
        execute_hive_insert(target_date, table_name)
    except Exception as e:
        print(f"[ERROR] 执行失败: {str(e)}")
