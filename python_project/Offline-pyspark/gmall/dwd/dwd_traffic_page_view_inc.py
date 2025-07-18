from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

# 1. 初始化SparkSession（全局单例模式）
def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("HiveETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE gmall")
    return spark

def select_to_hive(jdbcDF, tableName, partition_date):
    # 使用insertInto方法写入已存在的分区表
    jdbcDF.write \
        .mode('append') \
        .insertInto(f"gmall.{tableName}")


# 2. 执行Hive SQL插入操作
def execute_hive_insert(partition_date: str, tableName):
    spark = get_spark_session()

    # 构建SQL语句，修正字段别名以匹配Hive表结构
    select_sql = f"""
SELECT
    get_json_object(log, '$.common.ar') AS province_id,
    get_json_object(log, '$.common.ba') AS brand,
    get_json_object(log, '$.common.ch') AS channel,
    get_json_object(log, '$.common.is_new') AS is_new,
    get_json_object(log, '$.common.md') AS model,
    get_json_object(log, '$.common.mid') AS mid_id,
    get_json_object(log, '$.common.os') AS operate_system,
    get_json_object(log, '$.common.uid') AS user_id,
    get_json_object(log, '$.common.vc') AS version_code,
    get_json_object(log, '$.page.item') AS page_item,
    get_json_object(log, '$.page.item_type') AS page_item_type,
    get_json_object(log, '$.page.last_page_id') AS last_page_id,
    get_json_object(log, '$.page.page_id') AS page_id,
    get_json_object(log, '$.page.from_pos_id') AS from_pos_id,
    get_json_object(log, '$.page.from_pos_seq') AS from_pos_seq,
    get_json_object(log, '$.page.refer_id') AS refer_id,
    date_format(from_utc_timestamp(get_json_object(log, '$.ts'), "UTC"), 'yyyy-MM-dd') AS date_id,
    date_format(from_utc_timestamp(get_json_object(log, '$.ts'), "UTC"), 'yyyy-MM-dd HH:mm:ss') AS view_time,
    get_json_object(log, '$.common.sid') AS session_id,
    get_json_object(log, '$.page.during_time') AS during_time
FROM ods_z_log
WHERE dt = '20250701'
AND get_json_object(log, '$.page') IS NOT NULL;
    """

    # 执行SQL
    print(f"[INFO] 开始执行SQL插入，分区日期：{partition_date}")
    df1 = spark.sql(select_sql)

    # 将during_time字段转换为BIGINT类型
    df1 = df1.withColumn("during_time", col("during_time").cast("bigint"))

    # 添加分区字段
    df_with_partition = df1.withColumn("ds", lit(partition_date))

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show()

    # 写入Hive
    select_to_hive(df_with_partition, tableName, partition_date)


# 4. 主函数（示例调用）
if __name__ == "__main__":
    table_name = 'dwd_traffic_page_view_inc'
    # 设置目标分区日期
    target_date = '20250717'
    # 执行插入操作
    execute_hive_insert(target_date, table_name)