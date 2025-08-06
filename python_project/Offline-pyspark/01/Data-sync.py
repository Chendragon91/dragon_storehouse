#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MySQL到Hive数据同步工具(增强版)
"""

import os
import sys
import logging
import configparser
import pymysql
import datetime
import time
import subprocess
import platform
from typing import Dict, List, Tuple, Optional
from pyspark.sql import SparkSession

# 配置日志格式
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mysql_to_hive_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MySQLToHiveSync:
    """MySQL到Hive数据同步工具类"""

    def __init__(self, config_file: str = 'config.ini'):
        """初始化配置"""
        self.config_file = config_file
        self.mysql_config = {}
        self.hive_config = {}
        self.sync_config = {}

        # 先加载配置
        self.load_config()

        # 确保MySQL JDBC驱动可用
        self.ensure_mysql_jdbc_driver()

        # 然后创建Spark会话
        self.spark = self.create_spark_session()

        # 最后确保HDFS路径存在
        self.ensure_warehouse_structure()

    def ensure_mysql_jdbc_driver(self):
        """确保MySQL JDBC驱动可用"""
        try:
            # 检查是否已安装驱动
            import jpype
            jpype.startJVM()
            try:
                jpype.JClass('com.mysql.cj.jdbc.Driver')
                logger.info("MySQL JDBC驱动已存在")
            except Exception:
                logger.warning("未找到MySQL JDBC驱动，尝试下载...")
                # 尝试下载驱动
                driver_url = 'https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar'
                driver_path = 'mysql-connector-java-8.0.28.jar'

                if not os.path.exists(driver_path):
                    try:
                        import requests
                        response = requests.get(driver_url)
                        with open(driver_path, 'wb') as f:
                            f.write(response.content)
                        logger.info(f"已下载MySQL JDBC驱动到 {driver_path}")
                    except Exception as e:
                        logger.error(f"下载MySQL JDBC驱动失败: {str(e)}")
                        logger.info("请手动下载MySQL JDBC驱动并放在当前目录")

            jpype.shutdownJVM()
        except Exception as e:
            logger.warning(f"检查MySQL JDBC驱动时出错: {str(e)}")
            logger.info("假设驱动已存在或将会在运行时提供")

    def ensure_warehouse_structure(self):
        """确保HDFS仓库目录结构存在"""
        base_paths = [
            self.hive_config["hive_tmp_dir"],
            f"/warehouse/{self.hive_config['database']}",
            f"/warehouse/{self.hive_config['database']}/ods"
        ]

        for path in base_paths:
            try:
                self.ensure_hdfs_path(path)
            except Exception as e:
                logger.warning(f"无法确保路径 {path} 存在: {str(e)}")
                # 继续尝试其他路径

    def ensure_hdfs_path(self, path: str):
        """确保HDFS路径存在（不修改已有目录的权限）"""
        try:
            hadoop = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark._jsc.hadoopConfiguration()
            )
            hdfs_path = self.spark._jvm.org.apache.hadoop.fs.Path(path)

            if not hadoop.exists(hdfs_path):
                hadoop.mkdirs(hdfs_path)
                # 只对新创建的目录设置权限
                hadoop.setPermission(
                    hdfs_path,
                    self.spark._jvm.org.apache.hadoop.fs.permission.FsPermission("777")
                )
                logger.info(f"创建HDFS路径: {path}")
            else:
                logger.info(f"HDFS路径已存在: {path}")

        except Exception as e:
            logger.warning(f"处理HDFS路径 {path} 时遇到问题: {str(e)}")
            # 不抛出异常，继续执行

    def load_config(self):
        """加载配置文件"""
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(f"配置文件不存在: {self.config_file}")

        config = configparser.ConfigParser()
        config.read(self.config_file)

        # MySQL配置
        self.mysql_config = {
            "host": config.get('mysql', 'host'),
            "port": config.getint('mysql', 'port'),
            "user": config.get('mysql', 'user'),
            "password": config.get('mysql', 'password'),
            "database": config.get('mysql', 'database'),
            "charset": config.get('mysql', 'charset', fallback='utf8mb4')
        }

        # Hive配置
        self.hive_config = {
            "database": config.get('hive', 'database'),
            "metastore_uris": config.get('hive', 'metastore_uris'),
            "warehouse_dir": config.get('hive', 'warehouse_dir'),
            "hdfs_default_fs": config.get('hive', 'hdfs_default_fs'),
            "hive_tmp_dir": config.get('hive', 'hive_tmp_dir', fallback='/tmp/hive')
        }

        # 同步配置
        self.sync_config = {
            "sync_mode": config.get('sync', 'mode', fallback='full'),
            "incremental_column": config.get('sync', 'incremental_column', fallback=None),
            "partition_column": config.get('sync', 'partition_column', fallback='dt'),
            "parallelism": config.getint('sync', 'parallelism', fallback=2),
            "batch_size": config.getint('sync', 'batch_size', fallback=100),
            "retry_times": config.getint('sync', 'retry_times', fallback=3)
        }

        logger.info("配置加载完成")

    def create_spark_session(self) -> SparkSession:
        """创建Spark会话"""
        try:
            # 检查是否存在MySQL JDBC驱动
            driver_path = 'mysql-connector-java-8.0.28.jar'
            extra_class_path = []
            if os.path.exists(driver_path):
                extra_class_path.append(os.path.abspath(driver_path))
                logger.info(f"添加MySQL JDBC驱动到classpath: {driver_path}")

            spark_builder = SparkSession.builder \
                .appName(f"MySQL2Hive_Sync_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}") \
                .config("spark.sql.warehouse.dir", self.hive_config["warehouse_dir"]) \
                .config("hive.metastore.uris", self.hive_config["metastore_uris"]) \
                .config("spark.hadoop.fs.defaultFS", self.hive_config["hdfs_default_fs"]) \
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
                .config("spark.sql.shuffle.partitions", str(self.sync_config["parallelism"]))

            # 如果有额外的classpath，添加到配置中
            if extra_class_path:
                spark_builder = spark_builder.config("spark.driver.extraClassPath", ":".join(extra_class_path))
                spark_builder = spark_builder.config("spark.executor.extraClassPath", ":".join(extra_class_path))

            spark = spark_builder.enableHiveSupport().getOrCreate()

            logger.info("Spark会话创建成功")
            return spark
        except Exception as e:
            logger.error(f"创建Spark会话失败: {str(e)}")
            raise

    def get_mysql_connection(self):
        """获取MySQL连接"""
        return pymysql.connect(
            host=self.mysql_config["host"],
            port=self.mysql_config["port"],
            user=self.mysql_config["user"],
            password=self.mysql_config["password"],
            database=self.mysql_config["database"],
            charset=self.mysql_config["charset"]
        )

    def get_mysql_tables(self) -> List[str]:
        """获取MySQL中所有表名"""
        try:
            with self.get_mysql_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SHOW TABLES FROM {self.mysql_config['database']}")
                    return [table[0] for table in cursor.fetchall()]
        except Exception as e:
            logger.error(f"获取MySQL表列表失败: {str(e)}")
            raise

    def sync_table_data(self, table_name: str) -> bool:
        """同步单表数据"""
        max_retry = self.sync_config["retry_times"]

        for attempt in range(max_retry):
            try:
                # 获取增量条件(如果是增量同步)
                incremental_condition = ""
                if self.sync_config["sync_mode"] == "incremental" and self.sync_config["incremental_column"]:
                    last_partition = self.get_max_partition_value(table_name)
                    if last_partition:
                        incremental_condition = f"WHERE {self.sync_config['incremental_column']} > '{last_partition}'"

                # 获取表的列信息
                columns, _ = self.get_table_metadata(table_name)
                partition_column = self.sync_config['partition_column']

                # 检查MySQL表是否已存在同名分区列
                existing_partition_column = [col for col in columns if col[0] == partition_column]

                if existing_partition_column:
                    # 如果MySQL表中已存在同名分区列，需要明确指定列名并重命名分区列
                    column_names = [col[0] for col in columns]
                    # 构建SELECT子句，重命名冲突的分区列
                    select_columns = []
                    for col_name in column_names:
                        if col_name == partition_column:
                            select_columns.append(f"{col_name} AS original_{partition_column}")
                        else:
                            select_columns.append(col_name)
                    select_clause = ", ".join(select_columns)

                    query = f"""
                        (SELECT 
                            {select_clause}, 
                            '{datetime.datetime.now().strftime("%Y%m%d")}' AS {partition_column} 
                        FROM {table_name}
                        {incremental_condition}
                        ) tmp
                    """
                else:
                    # 如果MySQL表中不存在同名分区列，直接使用*通配符
                    query = f"""
                        (SELECT 
                            *, 
                            '{datetime.datetime.now().strftime("%Y%m%d")}' AS {partition_column} 
                        FROM {table_name}
                        {incremental_condition}
                        ) tmp
                    """

                # 读取MySQL数据
                df = self.spark.read.format("jdbc") \
                    .option("url", f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=UTC&zeroDateTimeBehavior=convertToNull") \
                    .option("dbtable", query) \
                    .option("user", self.mysql_config["user"]) \
                    .option("password", self.mysql_config["password"]) \
                    .option("fetchsize", str(self.sync_config["batch_size"])) \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .load()

                # 写入Hive
                df.write.mode("overwrite") \
                    .option("compression", "snappy") \
                    .partitionBy(partition_column) \
                    .saveAsTable(f"{self.hive_config['database']}.ods_{table_name}")

                logger.info(f"表 {table_name} 数据同步成功(尝试 {attempt+1}/{max_retry})")
                return True

            except Exception as e:
                logger.warning(f"表 {table_name} 数据同步失败(尝试 {attempt+1}/{max_retry}): {str(e)}")
                if attempt == max_retry - 1:
                    logger.error(f"表 {table_name} 数据同步最终失败")
                    return False
                # 等待后重试
                time.sleep(10 * (attempt + 1))

        return False

    def get_max_partition_value(self, table_name: str) -> Optional[str]:
        """获取Hive表最大分区值"""
        try:
            # 检查表是否存在
            df = self.spark.sql(f"""
                SHOW TABLES IN {self.hive_config['database']} LIKE 'ods_{table_name}'
            """)
            if df.count() == 0:
                return None

            df = self.spark.sql(f"""
                SHOW PARTITIONS {self.hive_config['database']}.ods_{table_name}
            """)

            if df.count() == 0:
                return None

            partitions = [row[0].split('=')[1] for row in df.collect()]
            return max(partitions)
        except Exception as e:
            logger.warning(f"获取表 {table_name} 分区信息失败: {str(e)}")
            return None

    def get_table_metadata(self, table_name: str) -> Tuple[List[Tuple], str]:
        """获取MySQL表元数据(列信息和表注释)"""
        try:
            with self.get_mysql_connection() as conn:
                # 获取列信息
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT COLUMN_NAME, DATA_TYPE, COLUMN_COMMENT 
                        FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_SCHEMA='{self.mysql_config['database']}' 
                          AND TABLE_NAME='{table_name}'
                        ORDER BY ORDINAL_POSITION
                    """)
                    columns = cursor.fetchall()

                # 获取表注释
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT TABLE_COMMENT 
                        FROM INFORMATION_SCHEMA.TABLES 
                        WHERE TABLE_SCHEMA='{self.mysql_config['database']}' 
                          AND TABLE_NAME='{table_name}'
                    """)
                    table_comment = cursor.fetchone()[0] or ""

                return columns, table_comment
        except Exception as e:
            logger.error(f"获取表 {table_name} 元数据失败: {str(e)}")
            raise

    def sync_all_data(self) -> bool:
        """同步所有表数据"""
        try:
            tables = self.get_mysql_tables()
            logger.info(f"开始同步 {len(tables)} 张表数据")

            success_count = 0
            for table in tables:
                if self.sync_table_data(table):
                    success_count += 1

            logger.info(f"数据同步完成，成功 {success_count}/{len(tables)}")
            return success_count == len(tables)
        except Exception as e:
            logger.error(f"同步数据失败: {str(e)}")
            return False

    def run(self, skip_schema_sync=True, recover_corrupted=False):
        """执行同步流程
        :param skip_schema_sync: 是否跳过表结构同步
        :param recover_corrupted: 是否尝试恢复损坏的表
        """
        try:
            logger.info("同步任务开始")

            # 1. 同步数据
            if not self.sync_all_data():
                raise Exception("数据同步失败")

            logger.info("所有同步任务完成")
            return True
        except Exception as e:
            logger.error(f"同步过程出错: {str(e)}")
            return False
        finally:
            self.spark.stop()

if __name__ == "__main__":
    # 示例配置文件内容
    config_content = """
[mysql]
host = cdh03
port = 3306
user = root
password = root
database = gmall_01
charset = utf8mb4

[hive]
database = gmall_01
metastore_uris = thrift://cdh01:9083
warehouse_dir = /user/hive/warehouse
hdfs_default_fs = hdfs://cdh01:8020
hive_tmp_dir = /tmp/hive

[sync]
mode = full
incremental_column = behavior_time
partition_column = dt
parallelism = 2
batch_size = 100
retry_times = 3
"""

    # 如果配置文件不存在，则创建
    if not os.path.exists('config.ini'):
        with open('config.ini', 'w') as f:
            f.write(config_content.strip())
        logger.info("已创建示例配置文件 config.ini")

    # 运行同步
    try:
        sync_tool = MySQLToHiveSync()
        if sync_tool.run(skip_schema_sync=True):
            logger.info("同步任务成功完成")
            sys.exit(0)
        else:
            logger.error("同步任务失败")
            sys.exit(1)
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        sys.exit(1)