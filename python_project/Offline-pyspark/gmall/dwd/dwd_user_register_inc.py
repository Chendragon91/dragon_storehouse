from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


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
with use_r as (
    select
        *
    from ods_user_info
    where dt='20250701'
),
    log as (
        select
            get_json_object(log,'$.common.ch') channel,
            get_json_object(log,'$.common.ar') province_id,
            get_json_object(log,'$.common.vc') version_code,
            get_json_object(log,'$.common.mid') mid_id,
            get_json_object(log,'$.common.ba') brand,
            get_json_object(log,'$.common.md') model,
            get_json_object(log,'$.common.os') operate_system,
            get_json_object(log,'$.common.uid') uid
        from ods_z_log
        WHERE dt = '20250701'
AND get_json_object(log, '$.page') IS NOT NULL
    )
select
    use_r.id,
    substr(create_time,1,7),
    create_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,model,
    operate_system
from use_r
left join log on use_r.id=log.uid;
    """

    # 执行SQL
    print(f"[INFO] 开始执行SQL插入，分区日期：{partition_date}")
    df1 = spark.sql(select_sql)

    # 添加分区字段
    df_with_partition = df1.withColumn("ds", lit(partition_date))

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show()

    # 写入Hive
    select_to_hive(df_with_partition, tableName, partition_date)


# 4. 主函数（示例调用）
if __name__ == "__main__":
    table_name = 'dwd_user_register_inc'
    # 设置目标分区日期
    target_date = '20250717'
    # 执行插入操作
    execute_hive_insert(target_date, table_name)