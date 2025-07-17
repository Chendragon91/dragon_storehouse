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
with sku as (
        select
            *
        from ods_sku_info
        where dt='20250701'
        ),
    spu as(
        select
            *
        from ods_spu_info
        where dt='20250701'
    ),
    c3 as(
        select
            *
        from ods_base_category3
        where dt='20250701'
    ),
    c2 as(
        select
            *
        from ods_base_category2
        where dt='20250701'
    ),
    c1 as ( 
        select 
            *
        from ods_base_category1
        where dt='20250701'
    ),
    tm as(
        select
            *
        from ods_base_trademark
        where dt='20250701'
    ),
    attr as (
        select
            *
        from ods_sku_attr_value
        where dt='20250701'
    ),
    sale_attr as (
        select
            *
        from ods_sku_sale_attr_value
        where dt='20250701'
    )
select
    sku.id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.is_sale,
    spu.id,
    spu.spu_name,
    c3.id,
    c3.name,
    c2.id,
    c2.name,
    c1.id,
    c1.name,
    tm.id,
    tm.tm_name,
    attr.id,
    attr.value_id,
    attr.attr_name,
    attr.value_name,
    sale_attr.id,
    sale_attr.sale_attr_value_id,
    sale_attr.sale_attr_name,
    sale_attr.sale_attr_value_name,
    sku.create_time
from sku
left join spu on sku.spu_id=spu.id
left join c3 on spu.category3_id=c3.id
left join c2 on c3.category2_id=c2.id
left join c1 on c2.category1_id=c1.id
left join tm on tm.id=sku.tm_id
left join attr on attr.sku_id=sku.id
left join sale_attr on sale_attr.sku_id=sku.id;
    """

    # 执行SQL
    print(f"[INFO] 开始执行SQL插入，分区日期：{partition_date}")
    df1 = spark.sql(select_sql)

    # 将is_sale字段从INT类型转换为BOOLEAN类型
    df1 = df1.withColumn("is_sale", col("is_sale").cast("boolean"))

    # 添加分区字段
    df_with_partition = df1.withColumn("ds", lit(partition_date))

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show()

    # 写入Hive
    select_to_hive(df_with_partition, tableName, partition_date)

# 4. 主函数（示例调用）
if __name__ == "__main__":
    table_name = 'dim_sku_full'
    # 设置目标分区日期
    target_date = '20250717'
    # 执行插入操作
    execute_hive_insert(target_date, table_name)