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

def select_to_hive(jdbcDF, tableName):
    # 使用insertInto方法写入已存在的分区表
    jdbcDF.write \
        .mode('append') \
        .insertInto(f"gmall.{tableName}")

# 2. 执行Hive SQL插入操作
def execute_hive_insert(partition_date: str, tableName):
    spark = get_spark_session()
    # 构建SQL语句：where条件固定为20250701，输出dt为传入的partition_date
    select_sql = f"""
select * from ads_coupon_stats
union
select
    '{partition_date}' dt,  -- 输出的分区字段使用传入的日期
    coupon_id,
    coupon_name,
    cast(sum(used_count_1d) as bigint) as used_count,
    cast(count(*) as bigint) as used_user_count
from dws_tool_user_coupon_coupon_used_1d
where dt='20250701'  -- 固定过滤20250701的数据（因为只有该日期的数据）
group by coupon_id,coupon_name;
    """
    # 执行SQL
    print(f"[INFO] 开始执行SQL插入，分区日期：{partition_date}")
    df1 = spark.sql(select_sql)
    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df1.show()
    # 写入Hive
    select_to_hive(df1, tableName)

# 4. 主函数（示例调用）
if __name__ == "__main__":
    table_name = 'ads_coupon_stats'
    # 设置目标分区日期（例如20250717）
    target_date = '20250717'
    # 执行插入操作
    execute_hive_insert(target_date, table_name)