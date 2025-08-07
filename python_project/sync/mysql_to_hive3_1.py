# -*- coding: utf-8 -*-
import os
import sys
import pymysql
import datetime
import tkinter as tk
from tkinter import messagebox
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# -------------------------- 配置项（在此处修改库名和连接信息） --------------------------
# 直接修改以下两个参数即可指定同步的库名
MYSQL_DB = "gmall_02"  # MySQL数据库名
HIVE_DB = "gmall_02"  # Hive数据库名

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
    """获取MySQL表字段信息（排除dt列）"""
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE, COLUMN_COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA='{mysql_db}' 
              AND TABLE_NAME='{table_name}'
              AND COLUMN_NAME != 'dt'  # 排除原表的dt列
            ORDER BY ORDINAL_POSITION
        """)
        return cursor.fetchall()

def generate_hive_table_ddl(hive_db, table_name, columns, table_comment=""):
    """
    生成Hive外部表DDL语句
    """
    # MySQL到Hive类型映射字典
    type_map = {
        'varchar': 'STRING',
        'char': 'STRING',
        'text': 'STRING',
        'longtext': 'STRING',
        'mediumtext': 'STRING',
        'tinytext': 'STRING',
        'tinyint': 'TINYINT',
        'smallint': 'SMALLINT',
        'int': 'INT',
        'integer': 'INT',
        'bigint': 'BIGINT',
        'float': 'FLOAT',
        'double': 'DOUBLE',
        'decimal': 'DECIMAL',
        'boolean': 'BOOLEAN',
        'tinyint(1)': 'BOOLEAN',
        'date': 'DATE',
        'datetime': 'TIMESTAMP',
        'timestamp': 'TIMESTAMP',
        'time': 'STRING',
        'year': 'INT',
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
def sync_table_data(mysql_db, hive_db, table_name, mysql_config, spark, overwrite=True):
    """
    同步单表数据（使用表中已有的dt列作为分区字段）

    参数:
        mysql_db: MySQL数据库名
        hive_db: Hive数据库名
        table_name: 表名
        mysql_config: MySQL连接配置
        spark: Spark会话
        overwrite: 是否覆盖已存在的分区(默认True)
    """
    conn = None
    try:
        conn = get_mysql_connection(mysql_config)

        # 1. 获取表列名（包含dt列）
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA='{mysql_db}' 
                  AND TABLE_NAME='{table_name}'
                ORDER BY ORDINAL_POSITION
            """)
            column_names = [col[0] for col in cursor.fetchall()]

        # 确保表中有dt列
        if 'dt' not in column_names:
            raise Exception(f"表 {table_name} 中没有dt列，无法使用已有日期作为分区字段")

        # 2. 配置MySQL连接
        mysql_url = f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/{mysql_db}?useSSL=false&serverTimezone=UTC"
        jdbc_properties = {
            "user": mysql_config["user"],
            "password": mysql_config["password"],
            "driver": "com.mysql.jdbc.Driver",
            "fetchsize": "1000"
        }

        # 3. 读取数据（直接使用原表数据，不添加任何新列）
        query = f"(SELECT * FROM {table_name}) as tmp"
        df = spark.read.jdbc(url=mysql_url, table=query, properties=jdbc_properties)

        # 4. 检查dt列是否存在且不为空
        if 'dt' not in df.columns:
            raise Exception(f"表 {table_name} 中没有dt列")

        # 5. 为每个不同的dt值创建分区
        distinct_dts = [row.dt for row in df.select('dt').distinct().collect() if row.dt]

        for dt in distinct_dts:
            try:
                # 筛选当前分区的数据
                partition_df = df.filter(df.dt == dt)

                # 关键修改：从数据中排除dt列，因为dt是分区列，不应该在SELECT列表中出现
                non_partition_columns = [c for c in partition_df.columns if c != 'dt']
                data_to_insert = partition_df.select(*[col(c) for c in non_partition_columns])

                # 写入模式选择
                write_mode = "overwrite" if overwrite else "append"

                # 写入Hive分区
                hdfs_path = f"/warehouse/{hive_db}/ods/ods_{table_name}/dt={dt}"

                # 在覆盖模式下，先删除已有分区
                if overwrite:
                    try:
                        spark.sql(f"ALTER TABLE {hive_db}.ods_{table_name} DROP IF EXISTS PARTITION (dt='{dt}')")
                    except:
                        pass  # 如果分区不存在则忽略错误

                # 关键修改：使用正确的INSERT语句，不包含dt列在SELECT中
                data_to_insert.createOrReplaceTempView(f"temp_{table_name}")

                columns_str = ", ".join(non_partition_columns)
                spark.sql(f"""
                    INSERT INTO TABLE {hive_db}.ods_{table_name} PARTITION(dt='{dt}')
                    SELECT {columns_str} FROM temp_{table_name}
                """)

                # 验证分区
                if not verify_partition(spark, hive_db, f"ods_{table_name}", dt):
                    raise Exception(f"分区验证失败: dt={dt}")

                print(f"✅ 同步成功 | 表: {table_name} | 分区: dt={dt} | 模式: {write_mode} | 路径: {hdfs_path}")

            except Exception as e:
                print(f"⚠️ 处理分区 dt={dt} 时出错: {str(e)}")
                continue

        return True

    except Exception as e:
        print(f"❌ 同步失败: {str(e)}")
        return False
    finally:
        if conn:
            conn.close()

def verify_partition(spark, db, table, dt):
    """修正后的分区验证逻辑"""
    try:
        # 检查元数据
        spark.sql(f"SHOW PARTITIONS {db}.{table} PARTITION(dt='{dt}')").collect()

        # 检查HDFS路径
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

def sync_all_data(mysql_db, hive_db, mysql_config, spark, overwrite=True):
    """同步所有表数据"""
    conn = None
    try:
        conn = get_mysql_connection(mysql_config)
        tables = get_mysql_tables(conn, mysql_db)
        print(f"\n===== 开始同步 {mysql_db} 到 {hive_db} 的数据，共 {len(tables)} 张表 =====")
        print(f"===== 覆盖模式: {'是' if overwrite else '否'} =====")

        success_count = 0
        for table in tables:
            if sync_table_data(mysql_db, hive_db, table, mysql_config, spark, overwrite):
                success_count += 1
            else:
                print(f"⚠️ 表 {table} 同步失败，继续处理下一张表...")

        print(f"\n===== 同步完成 =====")
        print(f"成功: {success_count}/{len(tables)}")
        print(f"失败: {len(tables)-success_count}/{len(tables)}")

        return success_count == len(tables)
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

        # 同步数据（默认使用覆盖模式）
        if sync_all_data(MYSQL_DB, HIVE_DB, mysql_config, spark, overwrite=True):
            messagebox.showinfo("成功", "所有表结构和数据同步完成！")
        else:
            messagebox.showwarning("部分成功", "数据同步存在失败项，请查看日志！")

    except Exception as e:
        error_msg = f"执行过程出错：{str(e)}"
        print(error_msg)
        messagebox.showerror("错误", error_msg)
    finally:
        if 'spark' in locals():
            spark.stop()
        sys.exit(0)

if __name__ == "__main__":
    main()
