from pyspark.sql import SparkSession
import pymysql
import logging
from typing import Dict, List, Tuple

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hive_mysql_sync.log'),
        logging.StreamHandler()
    ]
)

class HiveToMySQLSync:
    def __init__(self, spark: SparkSession, hive_db: str, mysql_config: Dict):
        self.spark = spark
        self.hive_db = hive_db
        self.mysql_config = mysql_config
        self.batch_size = 10000  # 批量插入记录数

    def get_mysql_connection(self):
        """获取MySQL连接并设置自动重连"""
        return pymysql.connect(
            host=self.mysql_config['host'],
            port=self.mysql_config['port'],
            user=self.mysql_config['user'],
            password=self.mysql_config['password'],
            database=self.mysql_config['database'],
            autocommit=True,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

    def get_hive_tables(self) -> List[str]:
        """获取Hive库中所有表名，排除临时表"""
        tables = [row.tableName for row in
                  self.spark.sql(f"SHOW TABLES IN {self.hive_db}").collect()
                  if not row.tableName.startswith('tmp_')]
        logging.info(f"发现 {len(tables)} 张待同步表")
        return tables

    def get_hive_table_schema(self, table_name: str) -> Tuple[List[Tuple], str]:
        """获取表结构和注释（已添加列名去重）"""
        # 获取列信息
        df = self.spark.sql(f"DESCRIBE {self.hive_db}.{table_name}")
        columns = []
        seen_columns = set()  # 用于记录已处理的列名

        for row in df.collect():
            if not row.col_name.startswith('#') and row.col_name not in seen_columns:
                seen_columns.add(row.col_name)
                comment = row.comment if hasattr(row, 'comment') else ''
                columns.append((row.col_name, row.data_type.lower(), comment))

        # 获取表注释
        table_comment = self.spark.sql(
            f"SHOW TABLE EXTENDED IN {self.hive_db} LIKE '{table_name}'"
        ).collect()[0][0] or ""

        return columns, table_comment

    def convert_data_type(self, hive_type: str, column_name: str = None) -> str:
        """增强版类型映射，处理Hive与MySQL之间的类型不兼容问题"""
        type_map = {
            'string': 'VARCHAR(512)',
            'varchar': 'VARCHAR(255)',
            'char': 'CHAR(50)',
            'int': 'INT',
            'bigint': 'BIGINT',
            'double': 'DOUBLE',
            'float': 'FLOAT',
            'boolean': 'TINYINT(1)',
            'timestamp': 'TIMESTAMP',
            'date': 'DATE',
            'binary': 'BLOB'
        }

        # 特殊处理 next_page_dist 列
        if column_name == 'next_page_dist':
            return 'LONGTEXT'

        if hive_type.startswith('decimal'):
            return hive_type.upper().replace('decimal', 'DECIMAL')
        return type_map.get(hive_type, 'TEXT')

    def generate_mysql_ddl(self, table_name: str, columns: List[Tuple], comment: str) -> str:
        """生成MySQL建表语句，包含主键检测"""
        column_defs = []
        primary_keys = []

        for col in columns:
            name, col_type, col_comment = col
            mysql_type = self.convert_data_type(col_type, name)  # 传递列名以进行特殊处理
            comment_clause = f" COMMENT '{col_comment}'" if col_comment else ''

            # 假设名为id的字段是主键
            if name.lower() == 'id':
                primary_keys.append(f"`{name}`")

            column_defs.append(f" `{name}` {mysql_type}{comment_clause}")

        # 添加主键约束
        if primary_keys:
            column_defs.append(f" PRIMARY KEY ({', '.join(primary_keys)})")

        column_defs_str = ',\n '.join(column_defs)
        return f"""
        CREATE TABLE IF NOT EXISTS {self.mysql_config['database']}.{table_name} (
            {column_defs_str}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='{comment}'
        """

    def sync_table_structure(self, table_name: str):
        """同步表结构到MySQL"""
        columns, comment = self.get_hive_table_schema(table_name)
        ddl = self.generate_mysql_ddl(table_name, columns, comment)

        try:
            with self.get_mysql_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {self.mysql_config['database']}.{table_name}")
                    cursor.execute(ddl)
            logging.info(f"表 {table_name} 结构同步成功")
        except Exception as e:
            logging.error(f"同步表 {table_name} 结构失败: {str(e)}")
            raise

    def sync_table_data(self, table_name: str):
        """批量同步数据到MySQL"""
        try:
            # 对于大表使用分批处理
            if table_name in ["dwd_page_visit_detail"]:
                self._sync_large_table_data(table_name)
            else:
                self._sync_normal_table_data(table_name)
        except Exception as e:
            logging.error(f"同步表 {table_name} 数据失败: {str(e)}")
            raise

    def _sync_normal_table_data(self, table_name: str):
        """同步普通大小的表数据"""
        try:
            df = self.spark.sql(f"SELECT * FROM {self.hive_db}.{table_name}")
            columns = df.schema.names
            rows = df.collect()

            self._batch_insert_to_mysql(table_name, columns, rows)
            logging.info(f"表 {table_name} 数据同步完成，记录数: {len(rows)}")
        except Exception as e:
            logging.error(f"同步表 {table_name} 数据失败: {str(e)}")
            raise

    def _sync_large_table_data(self, table_name: str):
        """同步大表数据，使用分批处理避免内存溢出"""
        try:
            # 先获取总记录数
            count_df = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {self.hive_db}.{table_name}")
            total_count = count_df.collect()[0]['cnt']

            batch_size = 50000  # 每批处理5万条记录
            offset = 0
            total_processed = 0

            while offset < total_count:
                # 分批读取数据
                df = self.spark.sql(f"""
                    SELECT * FROM {self.hive_db}.{table_name} 
                    LIMIT {batch_size} OFFSET {offset}
                """)

                # 获取列名
                columns = df.schema.names

                # 收集当前批次数据
                rows = df.collect()

                # 批量插入到MySQL
                self._batch_insert_to_mysql(table_name, columns, rows)

                processed = len(rows)
                total_processed += processed
                offset += batch_size

                logging.info(f"表 {table_name} 已处理 {total_processed}/{total_count} 条记录")

                # 如果当前批次少于batch_size，说明已经处理完所有数据
                if processed < batch_size:
                    break

            logging.info(f"表 {table_name} 数据同步完成，记录数: {total_processed}")
        except Exception as e:
            logging.error(f"同步大表 {table_name} 数据失败: {str(e)}")
            raise

    def _batch_insert_to_mysql(self, table_name: str, columns: List[str], rows: List):
        """批量插入数据到MySQL"""
        if not rows:
            return

        conn = pymysql.connect(
            host=self.mysql_config['host'],
            port=self.mysql_config['port'],
            user=self.mysql_config['user'],
            password=self.mysql_config['password'],
            database=self.mysql_config['database'],
            charset='utf8mb4'
        )
        cursor = conn.cursor()

        try:
            placeholders = ', '.join(['%s'] * len(columns))
            insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

            batch = []
            for row in rows:
                batch.append(tuple(row))
                if len(batch) >= self.batch_size:
                    cursor.executemany(insert_sql, batch)
                    conn.commit()
                    batch = []

            if batch:  # 插入剩余记录
                cursor.executemany(insert_sql, batch)
                conn.commit()
        finally:
            cursor.close()
            conn.close()

    def run_sync(self):
        """执行完整同步流程"""
        tables = self.get_hive_tables()
        for table in tables:
            try:
                logging.info(f"开始同步表 {table}")
                self.sync_table_structure(table)
                self.sync_table_data(table)
                logging.info(f"表 {table} 同步成功")
            except Exception:
                logging.error(f"表 {table} 同步失败，跳过继续处理其他表")
                continue

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("EnhancedHiveMySQLSync") \
        .config("spark.driver.memory", "8g") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.local.dir", "D:\linshi") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    config = {
        'host': 'cdh03',
        'port': 3306,
        'user': 'root',
        'password': 'root',
        'database': 'gmall_02_ads'
    }

    syncer = HiveToMySQLSync(spark, "gmall_02", config)
    syncer.run_sync()
    spark.stop()
