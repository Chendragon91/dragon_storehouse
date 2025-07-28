from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, md5, date_add, from_utc_timestamp, date_format, when

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("UserInfoETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark

def select_to_hive(jdbcDF, tableName, partition_date):
    """将DataFrame写入Hive分区表"""
    jdbcDF.write \
        .mode('append') \
        .insertInto(f"tms_dim.{tableName}")

def execute_user_info_etl(partition_date: str, tableName):
    """处理用户信息并写入目标分区"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理用户信息，目标分区日期：{partition_date}")
    dt = "20250713"  # 源数据分区日期

    # 读取源表数据并进行转换处理
    user_info = spark.table("tms.ods_user_info") \
        .filter((col("dt") == dt) & (col("is_deleted") == "0")) \
        .select(
        col("id"),
        col("login_name"),
        col("nick_name"),
        md5(col("passwd")).alias("passwd"),
        md5(col("real_name")).alias("realname"),
        # 修复：使用when()函数作为起始，而非直接调用Column.when()
        md5(
            when(
                col("phone_num").rlike('^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$'),
                col("phone_num")
            ).otherwise(lit(None))
        ).alias("phone_num"),
        # 修复：使用when()函数作为起始
        md5(
            when(
                col("email").rlike('^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$'),
                col("email")
            ).otherwise(lit(None))
        ).alias("email"),
        col("user_level"),
        date_add(lit("1970-01-01"), col("birthday").cast("int")).alias("birthday"),
        col("gender"),
        date_format(
            from_utc_timestamp(col("create_time").cast("bigint").cast("timestamp"), "UTC"),
            "yyyy-MM-dd"
        ).alias("start_date"),
        lit("9999-12-31").alias("end_date")
    )

    # 添加分区字段
    final_df = user_info.withColumn("ds", lit(partition_date))

    print(f"[INFO] 数据处理完成，准备写入分区{partition_date}")
    final_df.show(5)  # 预览5条数据

    # 写入Hive
    select_to_hive(final_df, tableName, partition_date)

if __name__ == "__main__":
    target_table = 'dim_user_zip'  # 目标表名，可根据实际情况修改
    target_partition = '20250725'  # 目标分区日期
    execute_user_info_etl(target_partition, target_table)