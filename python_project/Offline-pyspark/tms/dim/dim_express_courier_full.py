from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, md5
from pyspark.sql.types import StringType


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
    spark.sql("USE tms")
    return spark


def select_to_hive(jdbcDF, tableName, partition_date):
    # 使用insertInto方法写入已存在的分区表
    jdbcDF.write \
        .mode('append') \
        .insertInto(f"tms_dim.{tableName}")


# 2. 执行数据处理并插入Hive（算子实现）
def execute_hive_insert(partition_date: str, tableName):
    spark = get_spark_session()
    print(f"[INFO] 开始执行算子处理，分区日期：{partition_date}")

    # 对应子查询express_cor_info
    express_cor_info = spark.table("tms.ods_express_courier") \
        .filter((col("dt") == "20250713") & (col("is_deleted") == "0")) \
        .select(
        col("id"),
        col("emp_id"),
        col("org_id"),
        md5(col("working_phone")).alias("working_phone"),  # 对手机号进行MD5加密
        col("express_type")
    )

    # 对应子查询organ_info
    organ_info = spark.table("tms.ods_base_organ") \
        .filter((col("dt") == "20250713") & (col("is_deleted") == "0")) \
        .select(
        col("id"),
        col("org_name")
    )

    # 对应子查询dic_info
    dic_info = spark.table("tms.ods_base_dic") \
        .filter((col("dt") == "20250713") & (col("is_deleted") == "0")) \
        .select(
        col("id"),
        col("name")
    )

    # 执行关联操作
    # 第一步：关联express_cor_info和organ_info
    join_organ = express_cor_info.join(
        organ_info,
        express_cor_info["org_id"] == organ_info["id"],
        "inner"
    ).select(
        express_cor_info["id"],
        express_cor_info["emp_id"],
        express_cor_info["org_id"],
        organ_info["org_name"],
        express_cor_info["working_phone"],
        express_cor_info["express_type"]
    )

    # 第二步：关联字典表获取express_type_name
    final_df = join_organ.join(
        dic_info,
        join_organ["express_type"] == dic_info["id"],
        "inner"
    ).select(
        join_organ["id"],
        join_organ["emp_id"],
        join_organ["org_id"],
        join_organ["org_name"],
        join_organ["working_phone"],
        join_organ["express_type"],
        dic_info["name"].alias("express_type_name")  # 别名对应SQL中的express_type_name
    )

    # 添加分区字段
    df_with_partition = final_df.withColumn("ds", lit(partition_date))

    print(f"[INFO] 算子处理完成，分区{partition_date}操作成功")
    df_with_partition.show()

    # 写入Hive
    select_to_hive(df_with_partition, tableName, partition_date)


# 3. 主函数（示例调用）
if __name__ == "__main__":
    table_name = 'dim_express_courier_full'
    # 设置目标分区日期
    target_date = '20250725'
    # 执行插入操作
    execute_hive_insert(target_date, table_name)