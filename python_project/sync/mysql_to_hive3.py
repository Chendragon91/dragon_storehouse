import os
import sys
import pymysql
import datetime
import tkinter as tk
from tkinter import messagebox
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# -------------------------- 配置项（在此处修改库名和连接信息） --------------------------
# 直接修改以下两个参数即可指定同步的库名
MYSQL_DB = "gmall_09"  # MySQL数据库名
HIVE_DB = "gmall_09"  # Hive数据库名

DEFAULT_MYSQL_CONFIG = {
    "host": "192.168.142.130",
    "port": 3306,
    "user": "root",
    "password": "root",
    "database": MYSQL_DB  # 关联上面的MySQL库名
}

DEFAULT_HIVE_CONFIG = {
    "metastore_uris": "thrift://192.168.142.128:9083",
    "hdfs_default_fs": "hdfs://192.168.142.128:8020",
    "warehouse_dir": "/user/hive/warehouse"
}


# -------------------------- 工具函数 --------------------------
def get_mysql_connection(mysql_config):
    """获取MySQL连接"""
    return pymysql.connect(
        host=mysql_config["host"],
        port=mysql_config["port"],
        user=mysql_config["user"],
        password=mysql_config["password"],
        database=mysql_config["database"],
        charset="utf8mb4"
    )


def ensure_hdfs_path(spark, path):
    """确保HDFS路径存在"""
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    )
    hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    if not fs.exists(hdfs_path):
        fs.mkdirs(hdfs_path)


def get_mysql_tables(conn, mysql_db):
    """获取MySQL库中所有表名"""
    with conn.cursor() as cursor:
        cursor.execute(f"SHOW TABLES FROM {mysql_db}")
        return [table[0] for table in cursor.fetchall()]


# -------------------------- 建表相关函数 --------------------------
def get_table_comment(conn, mysql_db, table_name):
    """获取MySQL表注释"""
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT TABLE_COMMENT 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA='{mysql_db}' 
              AND TABLE_NAME='{table_name}'
        """)
        result = cursor.fetchone()
    return result[0] if result and result[0] else ""


def get_table_columns(conn, mysql_db, table_name):
    """获取MySQL表字段信息"""
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT COLUMN_NAME,
                   CASE
                       WHEN DATA_TYPE IN ('varchar','char','text','longtext','mediumtext') THEN 'STRING'
                       WHEN DATA_TYPE IN ('int','tinyint','smallint') THEN 'INT'
                       WHEN DATA_TYPE = 'bigint' THEN 'BIGINT'
                       WHEN DATA_TYPE = 'decimal' THEN CONCAT('DECIMAL(',NUMERIC_PRECISION,',',NUMERIC_SCALE,')')
                       WHEN DATA_TYPE IN ('datetime','date','timestamp') THEN 'STRING'
                       ELSE 'STRING'
                   END AS hive_type,
                   COLUMN_COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA='{mysql_db}' AND TABLE_NAME='{table_name}'
            ORDER BY ORDINAL_POSITION
        """)
        return cursor.fetchall()


# def generate_create_ddl(hive_db, table_name, columns, table_comment):
#     """生成Hive建表DDL"""
#     column_defs = []
#     for col in columns:
#         col_name, hive_type, col_comment = col
#         escaped_comment = col_comment.replace("'", "''") if col_comment else ""
#         comment_clause = f" COMMENT '{escaped_comment}'" if escaped_comment else ""
#         column_defs.append(f"    `{col_name}` {hive_type}{comment_clause}")
#     formatted_columns = ",\n".join(column_defs)
#
#     return f"""
# CREATE EXTERNAL TABLE IF NOT EXISTS {hive_db}.ods_{table_name} (
# {formatted_columns}
# ) COMMENT '{table_comment.replace("'", "''")}'
# PARTITIONED BY (`dt` STRING)
# ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
# LOCATION '/warehouse/{hive_db}/ods/ods_{table_name}/'
# TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')
# """
def generate_hive_table_ddl(hive_db, table_name, columns, table_comment=""):
    """
    生成Hive外部表DDL语句，处理MySQL到Hive的类型映射

    参数:
        hive_db (str): Hive数据库名
        table_name (str): 表名
        columns (list): 列定义列表，每个元素为(列名, MySQL类型, 列注释)
        table_comment (str): 表注释

    返回:
        str: 完整的Hive DDL语句
    """
    # MySQL到Hive类型映射字典
    type_map = {
        # 字符串类型
        'varchar': 'STRING',
        'char': 'STRING',
        'text': 'STRING',
        'longtext': 'STRING',
        'mediumtext': 'STRING',
        'tinytext': 'STRING',

        # 数值类型
        'tinyint': 'TINYINT',
        'smallint': 'SMALLINT',
        'int': 'INT',
        'integer': 'INT',
        'bigint': 'BIGINT',
        'float': 'FLOAT',
        'double': 'DOUBLE',
        'decimal': 'DECIMAL',

        # 布尔类型
        'boolean': 'BOOLEAN',
        'tinyint(1)': 'BOOLEAN',

        # 日期时间类型
        'date': 'DATE',
        'datetime': 'TIMESTAMP',
        'timestamp': 'TIMESTAMP',
        'time': 'STRING',
        'year': 'INT',

        # 二进制类型
        'blob': 'BINARY',
        'longblob': 'BINARY',
        'mediumblob': 'BINARY',
        'tinyblob': 'BINARY',
        'binary': 'BINARY',
        'varbinary': 'BINARY'
    }

    column_defs = []
    for col in columns:
        col_name, mysql_type, col_comment = col
        escaped_comment = col_comment.replace("'", "''").replace("\n", " ") if col_comment else ""
        comment_clause = f" COMMENT '{escaped_comment}'" if escaped_comment else ""

        # 处理类型映射
        mysql_type_lower = mysql_type.lower()
        hive_type = type_map.get(mysql_type_lower, 'STRING')

        # 特殊处理decimal类型
        if mysql_type_lower.startswith('decimal'):
            precision_scale = mysql_type[mysql_type.index('('):] if '(' in mysql_type else '(10,2)'
            hive_type = f'DECIMAL{precision_scale}'

        # 特殊处理带长度的varchar/char
        elif mysql_type_lower.startswith(('varchar(', 'char(')):
            hive_type = 'STRING'  # Hive不保留长度信息

        column_defs.append(f"    `{col_name}` {hive_type}{comment_clause}")

    formatted_columns = ",\n".join(column_defs)
    escaped_table_comment = table_comment.replace("'", "''").replace("\n", " ") if table_comment else ""

    return f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {hive_db}.ods_{table_name} (
{formatted_columns}
) COMMENT '{escaped_table_comment}'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\\t'
  LINES TERMINATED BY '\\n'
  NULL DEFINED AS 'NULL'
STORED AS TEXTFILE
LOCATION '/warehouse/{hive_db}/ods/ods_{table_name}/'
TBLPROPERTIES (
  'compression.codec'='org.apache.hadoop.io.compress.GzipCodec',
  'skip.header.line.count'='1',
  'serialization.null.format'='NULL'
)
"""

def sync_schema(mysql_db, hive_db, mysql_config, spark):
    """同步表结构到Hive"""
    ddl_statements = []
    conn = None
    try:
        conn = get_mysql_connection(mysql_config)
        tables = get_mysql_tables(conn, mysql_db)
        print(f"\n===== 开始同步 {mysql_db} 到 {hive_db} 的表结构，共 {len(tables)} 张表 =====")

        for table in tables:
            try:
                columns = get_table_columns(conn, mysql_db, table)
                table_comment = get_table_comment(conn, mysql_db, table)
                create_ddl = generate_hive_table_ddl(hive_db, table, columns, table_comment)
                ddl_statements.append(create_ddl)

                hdfs_path = f"/warehouse/{hive_db}/ods/ods_{table}"
                ensure_hdfs_path(spark, hdfs_path)

                spark.sql(create_ddl)
                print(f"✅ 表 {hive_db}.ods_{table} 结构同步完成")
            except Exception as e:
                print(f"❌ 处理表 {table} 时出错（建表阶段）：{str(e)}")
                continue

    finally:
        if conn:
            conn.close()

    print(f"\n===== 生成的Hive建表语句如下 =====")
    for idx, ddl in enumerate(ddl_statements, 1):
        print(f"\n-- 第 {idx} 张表 --")
        print(ddl.strip())

    return ddl_statements


# -------------------------- 数据同步相关函数 --------------------------
# def sync_table_data(mysql_db, hive_db, table_name, mysql_config, spark):
#     """同步单表数据"""
#     try:
#         mysql_url = f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/{mysql_db}?useSSL=false"
#         jdbc_properties = {
#             "user": mysql_config["user"],
#             "password": mysql_config["password"],
#             "driver": "com.mysql.jdbc.Driver",
#             "fetchsize": "1000"
#         }
#
#         dt = datetime.datetime.now().strftime("%Y%m%d")
#         query = f"(SELECT *, '{dt}' as dt FROM {table_name}) as tmp"
#         df = spark.read.jdbc(
#             url=mysql_url,
#             table=query,
#             properties=jdbc_properties
#         )
#
#         df.write.mode("overwrite") \
#             .partitionBy("dt") \
#             .saveAsTable(f"{hive_db}.ods_{table_name}")
#
#         print(f"✅ 表 {mysql_db}.{table_name} 数据同步至 {hive_db}.ods_{table_name}（dt={dt}）完成")
#         return True
#     except Exception as e:
#         print(f"❌ 表 {table_name} 数据同步失败：{str(e)}")
#         return False
# def sync_table_data(mysql_db, hive_db, table_name, mysql_config, spark):
#     """同步单表数据（含元数据更新）"""
#     try:
#         # 1. 配置MySQL连接
#         mysql_url = f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/{mysql_db}?useSSL=false"
#         jdbc_properties = {
#             "user": mysql_config["user"],
#             "password": mysql_config["password"],
#             "driver": "com.mysql.jdbc.Driver",
#             "fetchsize": "1000"
#         }
#
#         # 2. 准备分区值
#         dt = datetime.datetime.now().strftime("%Y%m%d")
#         query = f"(SELECT *, '{dt}' as dt FROM {table_name}) as tmp"
#
#         # 3. 读取MySQL数据
#         df = spark.read.jdbc(
#             url=mysql_url,
#             table=query,
#             properties=jdbc_properties
#         )
#
#         # 4. 定义HDFS存储路径
#         hdfs_path = f"/warehouse/{hive_db}/ods/ods_{table_name}/dt={dt}"
#
#         # 5. 写入数据并更新元数据
#         (df.write
#          .mode("overwrite")
#          .partitionBy("dt")
#          .option("path", hdfs_path)  # 显式指定存储路径
#          .saveAsTable(f"{hive_db}.ods_{table_name}"))
#
#         # 6. 强制更新元数据
#         spark.sql(f"MSCK REPAIR TABLE {hive_db}.ods_{table_name}")
#
#         print(f"✅ 表 {mysql_db}.ods_{table_name} 同步完成 | 路径: ods_{hdfs_path}")
#
#         verify_partition(spark, hive_db, f"ods_{table_name}", dt)
#
#
#     except Exception as e:
#         print(f"❌ 同步失败: {str(e)}")
#         return False
#
#
# def verify_partition(spark, db, table, dt):
#     """验证分区元数据和HDFS路径"""
#     try:
#         # 检查元数据
#         spark.sql(f"SHOW PARTITIONS {db}.{table} PARTITION(dt='{dt}')").collect()
#
#         # 检查HDFS路径（需Hadoop客户端支持）
#         hdfs_path = f"/warehouse/{db}/ods/ods_{table}/dt={dt}"
#         hadoop = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
#             spark._jsc.hadoopConfiguration()
#         )
#         if not hadoop.exists(spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)):
#             raise Exception(f"HDFS路径不存在: {hdfs_path}")
#
#         return True
#     except Exception as e:
#         print(f"❌ 验证失败: {str(e)}")
#         return False

def sync_table_data(mysql_db, hive_db, table_name, mysql_config, spark):
    """同步单表数据（含元数据更新）"""
    try:
        # 1. 配置MySQL连接（8.x驱动）
        mysql_url = f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/{mysql_db}?useSSL=false&serverTimezone=UTC"
        jdbc_properties = {
            "user": mysql_config["user"],
            "password": mysql_config["password"],
            "driver": "com.mysql.jdbc.Driver",
            "fetchsize": "1000"
        }

        # 2. 准备分区值
        dt = datetime.datetime.now().strftime("%Y%m%d")
        query = f"(SELECT *, '{dt}' as dt FROM {table_name}) as tmp"

        # 3. 读取数据
        df = spark.read.jdbc(url=mysql_url, table=query, properties=jdbc_properties)

        # 4. 写入Hive
        hdfs_path = f"/warehouse/{hive_db}/ods/ods_{table_name}/dt={dt}"
        (df.write
         .mode("overwrite")
         .partitionBy("dt")
         .option("path", hdfs_path)
         .saveAsTable(f"{hive_db}.ods_{table_name}"))

        # 5. 验证分区
        if not verify_partition(spark, hive_db, f"ods_{table_name}", dt):
            raise Exception("分区验证失败")

        print(f"✅ 同步成功 | 路径: {hdfs_path}")
        return True

    except Exception as e:
        print(f"❌ 同步失败: {str(e)}")
        return False


def verify_partition(spark, db, table, dt):
    """修正后的分区验证逻辑"""
    try:
        # 检查元数据
        spark.sql(f"SHOW PARTITIONS {db}.{table} PARTITION(dt='{dt}')").collect()

        # 检查HDFS路径（修正路径拼接）
        hdfs_path = f"/warehouse/{db}/ods/{table}/dt={dt}"
        hadoop = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jsc.hadoopConfiguration()
        )
        if not hadoop.exists(spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)):
            raise Exception(f"HDFS路径不存在: {hdfs_path}")
        return True
    except Exception as e:
        print(f"❌ 验证失败: {str(e)}")
        return False


def sync_all_data(mysql_db, hive_db, mysql_config, spark):
    """同步所有表数据"""
    conn = None
    try:
        conn = get_mysql_connection(mysql_config)
        tables = get_mysql_tables(conn, mysql_db)
        print(f"\n===== 开始同步 {mysql_db} 到 {hive_db} 的数据，共 {len(tables)} 张表 =====")

        for table in tables:
            if not sync_table_data(mysql_db, hive_db, table, mysql_config, spark):
                print(f"❌ 数据同步中断：表 ods_{table} 同步失败")
                return False
        return True
    finally:
        if conn:
            conn.close()


# -------------------------- Spark会话配置 --------------------------
def create_spark_session(hive_config):
    """创建Spark会话"""
    jdbc_jar = r"F:\cdh\06Mysql\mysql-connector-java-5.1.27-bin.jar"
    if not os.path.exists(jdbc_jar):
        raise FileNotFoundError(f"驱动文件不存在：{jdbc_jar}")
    return SparkSession.builder \
        .appName(f"MySQL2Hive_Sync_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}") \
        .config("spark.sql.warehouse.dir", hive_config["warehouse_dir"]) \
        .config("spark.jars", jdbc_jar) \
        .config("hive.metastore.uris", hive_config["metastore_uris"]) \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.cleaner.referenceTracking.blocking", "false") \
        .config("spark.cleaner.periodicGC.interval", "1min") \
        .config("spark.hadoop.fs.defaultFS", hive_config["hdfs_default_fs"]) \
        .config("spark.local.dir", "E:/spark_temp") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
        .config("spark.sql.adaptive.enabled", "true") \
        .enableHiveSupport() \
        .getOrCreate()


# -------------------------- 主函数 --------------------------
def main():
    # 隐藏tkinter主窗口
    root = tk.Tk()
    root.withdraw()

    try:
        # 初始化配置
        mysql_config = DEFAULT_MYSQL_CONFIG.copy()
        hive_config = DEFAULT_HIVE_CONFIG.copy()

        # 创建Spark会话
        spark = create_spark_session(hive_config)

        # 同步表结构
        sync_schema(MYSQL_DB, HIVE_DB, mysql_config, spark)

        # 同步数据
        if sync_all_data(MYSQL_DB, HIVE_DB, mysql_config, spark):
            messagebox.showinfo("成功", "所有表结构和数据同步完成！")
        else:
            messagebox.showerror("失败", "数据同步存在失败项！")

    except Exception as e:
        error_msg = f"执行过程出错：{str(e)}"
        print(error_msg)
        messagebox.showerror("错误", error_msg)

    finally:
        if 'spark' in locals():
            spark.stop()
        # 确保程序正常退出
        sys.exit(0)


if __name__ == "__main__":
    main()