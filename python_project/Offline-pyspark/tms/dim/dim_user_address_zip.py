from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, md5, when, concat, substring

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("UserAddressETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    return spark

def create_target_table(spark, table_name):
    """创建目标Hive表（如果不存在）"""
    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS tms_dim;
    """)

    # 检查表是否存在
    table_exists = spark._jsparkSession.catalog().tableExists("tms_dim", table_name)

    if not table_exists:
        print(f"[INFO] 目标表 tms_dim.{table_name} 不存在，开始创建...")
        spark.sql(f"""
            CREATE TABLE tms_dim.{table_name} (
                id BIGINT,
                user_id BIGINT,
                phone STRING,
                province_id STRING,
                city_id STRING,
                district_id STRING,
                complex_id STRING,
                address STRING,
                is_default STRING,
                start_date STRING,
                end_date STRING
            )
            PARTITIONED BY (ds STRING)
            STORED AS ORC
            TBLPROPERTIES ('orc.compress'='SNAPPY');
        """)
        print(f"[INFO] 目标表 tms_dim.{table_name} 创建成功")
    else:
        print(f"[INFO] 目标表 tms_dim.{table_name} 已存在")

def select_to_hive(jdbcDF, tableName, partition_date):
    """将DataFrame写入Hive分区表"""
    jdbcDF.write \
        .mode('append') \
        .insertInto(f"tms_dim.{tableName}")  # 移除partitionBy，避免冲突

def execute_hive_insert(partition_date: str, tableName):
    """主处理逻辑：处理用户地址数据并写入目标分区"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理用户地址信息，目标分区日期：{partition_date}")

    # 确保目标表存在
    create_target_table(spark, tableName)

    dt = "20250713"  # 源数据分区日期

    # 1. 读取ods_user_address表数据
    address_df = spark.table("tms.ods_user_address") \
        .filter((col("dt") == dt) & (col("is_deleted") == "0")) \
        .select(
        col("id"),
        col("user_id"),
        col("phone"),
        col("province_id"),
        col("city_id"),
        col("district_id"),
        col("complex_id"),
        col("address"),
        col("is_default"),
        col("create_time")
    )

    # 2. 数据处理转换
    processed_df = address_df.select(
        col("id"),
        col("user_id"),
        # 手机号正则验证并MD5加密
        md5(
            when(
                col("phone").rlike('^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$'),
                col("phone")
            ).otherwise(None)
        ).alias("phone"),
        col("province_id"),
        col("city_id"),
        col("district_id"),
        col("complex_id"),
        col("address"),
        col("is_default"),
        # 格式化创建时间
        concat(
            substring(col("create_time"), 1, 10),
            lit(" "),
            substring(col("create_time"), 12, 8)
        ).alias("start_date"),
        # 固定结束日期
        lit("9999-12-31").alias("end_date")
    )

    # 添加分区字段
    final_df = processed_df.withColumn("ds", lit(partition_date))

    print(f"[INFO] 数据处理完成，准备写入分区{partition_date}")
    final_df.show(5)  # 预览5条数据

    # 写入Hive
    select_to_hive(final_df, tableName, partition_date)
    print(f"[INFO] 数据成功写入 tms_dim.{tableName} 分区 {partition_date}")

if __name__ == "__main__":
    table_name = 'dim_user_address_zip'  # 目标表名
    target_date = '20250725'  # 目标分区日期
    execute_hive_insert(target_date, table_name)