from pyspark.sql import SparkSession
import logging
import os
from typing import List, Optional

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hive_to_csv.log'),
        logging.StreamHandler()
    ]
)

class HiveToCSVExporter:
    def __init__(self, spark: SparkSession, hive_db: str, output_dir: str):
        self.spark = spark
        self.hive_db = hive_db
        self.output_dir = output_dir
        # 如果是本地路径，确保目录存在
        if not output_dir.startswith(('hdfs://', 's3://', 'file://')):
            os.makedirs(self.output_dir, exist_ok=True)

    def get_hive_tables(self) -> List[str]:
        """获取Hive库中所有表名，排除临时表"""
        tables = [row.tableName for row in
                  self.spark.sql(f"SHOW TABLES IN {self.hive_db}").collect()
                  if not row.tableName.startswith('tmp_')]
        logging.info(f"发现 {len(tables)} 张待导出表")
        return tables

    def export_table_to_csv(self, table_name: str, partition_filter: Optional[str] = None):
        """
        导出Hive表到CSV文件
        :param table_name: 表名
        :param partition_filter: 分区过滤条件，如"dt='20230801'"
        """
        try:
            # 构建查询SQL
            query = f"SELECT * FROM {self.hive_db}.{table_name}"
            if partition_filter:
                query += f" WHERE {partition_filter}"

            # 获取数据
            df = self.spark.sql(query)

            # 设置输出路径
            output_path = os.path.join(self.output_dir, table_name)

            # 导出为CSV
            (df.write
             .option("header", "true")
             .option("delimiter", ",")
             .option("quote", '"')
             .option("escape", '"')
             .option("encoding", "UTF-8")
             .mode("overwrite")
             .csv(output_path))

            logging.info(f"表 {table_name} 导出成功，保存路径: {output_path}")

        except Exception as e:
            logging.error(f"导出表 {table_name} 失败: {str(e)}")
            raise

    def export_all_tables(self, partition_filter: Optional[str] = None):
        """导出所有表到CSV"""
        tables = self.get_hive_tables()
        for table in tables:
            try:
                self.export_table_to_csv(table, partition_filter)
            except Exception as e:
                logging.error(f"表 {table} 导出失败，跳过继续处理其他表: {str(e)}")
                continue

if __name__ == "__main__":
    spark = None
    try:
        # 初始化SparkSession
        spark = SparkSession.builder \
            .appName("HiveToCSVExport") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .config("hive.metastore.uris", "thrift://cdh01:9083") \
            .config("spark.hadoop.fs.defaultFS", "file:///") \
            .enableHiveSupport() \
            .getOrCreate()

        # 配置参数
        hive_db = "gmall_09"
        output_dir = "D:/linshi"  # 本地输出目录
        partition_filter = "dt='20250801'"  # 可选的分区过滤条件

        # 执行导出
        exporter = HiveToCSVExporter(spark, hive_db, output_dir)
        exporter.export_all_tables(partition_filter)

    except Exception as e:
        logging.error(f"导出过程发生严重错误: {str(e)}", exc_info=True)
    finally:
        try:
            if spark:
                spark.stop()
        except Exception:
            pass
        logging.info("导出任务结束")
