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
select * from ads_user_change
union
select
    churn.dt,
    user_churn_count,
    user_back_count
from
(
    select
        '20250717' dt,
        count(*) user_churn_count
    from dws_user_user_login_td
    where dt='20250701'
    and login_date_last=date_add('20250701',-7)
)churn
join
(
    select
        '20250701' dt,
        count(*) user_back_count
    from
    (
        select
            user_id,
            login_date_last
        from dws_user_user_login_td
        where dt='20250701'
        and login_date_last = '20250701'
    )t1
    join
    (
        select
            user_id,
            login_date_last login_date_previous
        from dws_user_user_login_td
        where dt=date_add('20250701',-1)
    )t2
    on t1.user_id=t2.user_id
    where datediff(login_date_last,login_date_previous)>=8
)back
on churn.dt=back.dt;
"""

    # 执行SQL
    print(f"[INFO] 开始执行SQL插入，分区日期：{partition_date}")
    df1 = spark.sql(select_sql)

    # 添加分区段
    df_with_partition = df1.withColumn("ds", lit(partition_date))

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show()

    # 移除ds列
    df_to_insert = df_with_partition.drop("ds")

    # 写入Hive
    select_to_hive(df_to_insert, tableName, partition_date)


# 4. 主函数（示例调用）
if __name__ == "__main__":
    table_name = 'ads_user_change'
    # 设置目标分区日期
    target_date = '20250717'
    # 执行插入操作
    execute_hive_insert(target_date, table_name)