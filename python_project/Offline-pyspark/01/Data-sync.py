import pymysql
# 条件导入hive模块
# 首先检查是否安装了必要的依赖
import importlib
import sys

dependencies = {
    'sasl': 'sasl',
    'thrift': 'thrift',
    'thrift_sasl': 'thrift-sasl',
    'pyhive': 'pyhive[hive]'
}

missing_deps = []
# 兼容Python 3.4以下版本的依赖检查
for dep, package in dependencies.items():
    try:
        # 尝试导入模块
        __import__(dep)
    except ImportError:
        missing_deps.append(package)

HIVE_AVAILABLE = False
if not missing_deps:
    try:
        from pyhive import hive
        HIVE_AVAILABLE = True
    except ImportError:
        HIVE_AVAILABLE = False
        print("警告: 未安装pyhive，将跳过Hive数据同步")
    except Exception as e:
        HIVE_AVAILABLE = False
        print(f"警告: Hive模块导入失败: {e}")
else:
    print(f"警告: 缺少Hive连接依赖: {', '.join(missing_deps)}")
    print("请运行以下命令安装依赖:")
    print(f"pip install {' '.join(missing_deps)}")
    print("将跳过Hive数据同步")

import traceback
from datetime import datetime

# MySQL连接配置
mysql_config = {
    'user': 'root',
    'password': 'root',
    'host': 'cdh03',
    'database': 'gmall_01',
    'charset': 'utf8'
}

# Hive连接配置（只有在hive可用时才使用）
hive_config = {
    'host': 'cdh01',
    'port': 10000,
    'database': 'gmall_01'
}

# 日期范围
date_range = ['2025-08-05', '2025-08-06', '2025-08-07']

def sync_table_data(cursor, query, table_name, dt):
    """执行查询并返回结果"""
    try:
        cursor.execute(query)
        return cursor.fetchall()
    except Exception as e:
        print(f"查询{table_name}数据失败 (dt={dt}): {e}")
        return []

def insert_hive_data(cursor, insert_sql, values, table_name, dt):
    """插入数据到Hive表"""
    if cursor is None:
        return False
    try:
        cursor.execute(insert_sql, values)
        return True
    except Exception as e:
        print(f"插入{table_name}数据失败 (dt={dt}): {e}")
        return False

# 添加Spark相关配置和导入
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DecimalType
    # 导入Python内置Decimal类型用于类型检查
    from decimal import Decimal
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("警告: 未安装pyspark，将跳过Hive数据同步")
except Exception as e:
    SPARK_AVAILABLE = False
    print(f"警告: Spark模块导入失败: {e}")

# MySQL连接配置
mysql_config = {
    'user': 'root',
    'password': 'root',
    'host': 'cdh03',
    'database': 'gmall_01',
    'charset': 'utf8'
}

# Hive配置
hive_config = {
    'database': 'gmall_01',
    'warehouse_dir': '/user/hive/warehouse',
    'metastore_uri': f"thrift://cdh01:9083"
}

# 日期范围
date_range = ['2025-08-05', '2025-08-06', '2025-08-07']

def sync_table_data(cursor, query, table_name, dt):
    """执行查询并返回结果"""
    try:
        cursor.execute(query)
        return cursor.fetchall()
    except Exception as e:
        print(f"查询{table_name}数据失败 (dt={dt}): {e}")
        return []

def create_spark_session():
    """创建Spark会话"""
    try:
        # 获取当前Python解释器路径
        python_path = sys.executable
        print(f"使用Python路径: {python_path}")

        spark = SparkSession.builder \
            .appName("DataSync") \
            .config("spark.sql.warehouse.dir", hive_config['warehouse_dir']) \
            .config("hive.metastore.uris", hive_config['metastore_uri']) \
            .config("spark.pyspark.python", python_path) \
            .config("spark.pyspark.driver.python", python_path) \
            .enableHiveSupport() \
            .getOrCreate()
        print("成功创建Spark会话")
        return spark
    except Exception as e:
        print(f"创建Spark会话失败: {e}")
        return None

def get_table_schema(table_name):
    """获取表结构"""
    schemas = {
        'product_category': StructType([
            StructField('category_id', StringType(), True),
            StructField('category_name', StringType(), True),
            StructField('parent_id', StringType(), True),
            StructField('is_leaf', IntegerType(), True),
            StructField('dt', StringType(), True)
        ]),
        'product_info': StructType([
            StructField('product_id', StringType(), True),
            StructField('product_name', StringType(), True),
            StructField('category_id', StringType(), True),
            # 将FloatType改为DecimalType
            StructField('price', DecimalType(10, 2), True),
            StructField('status', StringType(), True),
            StructField('dt', StringType(), True)
        ]),
        'user_behavior': StructType([
            StructField('log_id', StringType(), True),
            StructField('user_id', StringType(), True),
            StructField('visitor_id', StringType(), True),
            StructField('product_id', StringType(), True),
            StructField('behavior_type', StringType(), True),
            StructField('behavior_time', TimestampType(), True),
            StructField('stay_duration', IntegerType(), True),
            StructField('terminal_type', StringType(), True),
            StructField('page_type', StringType(), True),
            StructField('click_behavior', StringType(), True),
            StructField('dt', StringType(), True)
        ]),
        'order_info': StructType([
            StructField('order_id', StringType(), True),
            StructField('user_id', StringType(), True),
            # 将FloatType改为DecimalType
            StructField('payment_amount', DecimalType(10, 2), True),
            StructField('payment_time', TimestampType(), True),
            StructField('order_status', StringType(), True),
            StructField('terminal_type', StringType(), True),
            StructField('is_new_buyer', IntegerType(), True),
            StructField('dt', StringType(), True)
        ]),
        'order_detail': StructType([
            StructField('order_id', StringType(), True),
            StructField('product_id', StringType(), True),
            StructField('product_name', StringType(), True),
            StructField('category_id', StringType(), True),
            # 将FloatType改为DecimalType
            StructField('price', DecimalType(10, 2), True),
            StructField('quantity', IntegerType(), True),
            StructField('dt', StringType(), True)
        ]),
        'micro_detail_visit': StructType([
            StructField('visit_id', StringType(), True),
            StructField('visitor_id', StringType(), True),
            StructField('product_id', StringType(), True),
            StructField('stay_duration', IntegerType(), True),
            StructField('visit_time', TimestampType(), True),
            StructField('dt', StringType(), True)
        ]),
        'order_refund': StructType([
            StructField('refund_id', StringType(), True),
            StructField('order_id', StringType(), True),
            StructField('product_id', StringType(), True),
            # 将FloatType改为DecimalType
            StructField('refund_amount', DecimalType(10, 2), True),
            StructField('refund_type', StringType(), True),
            StructField('refund_time', TimestampType(), True),
            StructField('dt', StringType(), True)
        ]),
        'marketing_activity': StructType([
            StructField('activity_id', StringType(), True),
            StructField('order_id', StringType(), True),
            StructField('activity_type', StringType(), True),
            # 将FloatType改为DecimalType
            StructField('discount_amount', DecimalType(10, 2), True),
            StructField('dt', StringType(), True)
        ]),
        'product_favorite': StructType([
            StructField('fav_id', StringType(), True),
            StructField('user_id', StringType(), True),
            StructField('product_id', StringType(), True),
            StructField('fav_time', TimestampType(), True),
            StructField('is_cancel', IntegerType(), True)
        ]),
        'shopping_cart': StructType([
            StructField('cart_id', StringType(), True),
            StructField('user_id', StringType(), True),
            StructField('product_id', StringType(), True),
            StructField('quantity', IntegerType(), True),
            StructField('cart_time', TimestampType(), True),
            StructField('is_delete', IntegerType(), True)
        ]),
        'product_competitiveness': StructType([
            StructField('product_id', StringType(), True),
            StructField('score', FloatType(), True),
            StructField('score_time', TimestampType(), True),
            StructField('score_dimensions', StringType(), True)
        ]),
        'yearly_payment_snapshot': StructType([
            StructField('product_id', StringType(), True),
            StructField('year_payment_amount', FloatType(), True),
            StructField('snapshot_date', StringType(), True)
        ])
    }
    return schemas.get(table_name)

def sync_data_to_hive():
    """将MySQL数据同步到Hive ODS层"""
    mysql_conn = None
    mysql_cursor = None
    spark = None

    try:
        # 连接MySQL
        print("正在连接MySQL...")
        mysql_conn = pymysql.connect(**mysql_config)
        mysql_cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
        print("MySQL连接成功")

        # 检查是否可以连接Hive
        hive_connected = False
        if SPARK_AVAILABLE:
            print("正在创建Spark会话...")
            spark = create_spark_session()
            if spark:
                print("Hive连接成功")
                hive_connected = True
            else:
                print("Spark会话创建失败，将跳过Hive数据同步")
        else:
            print("Spark功能不可用，将跳过Hive数据同步")

        print("开始数据同步...")

        # 使用统一的处理函数同步所有表
        tables = [
            'product_category',
            'product_info',
            'user_behavior',
            'order_info',
            'order_detail',
            'micro_detail_visit',
            'order_refund',
            'marketing_activity',
            'product_favorite',
            'shopping_cart',
            'product_competitiveness',
            'yearly_payment_snapshot'
        ]

        for table_name in tables:
            print(f"同步{table_name}表...")
            process_and_write_table(
                table_name,
                f"SELECT * FROM {table_name} WHERE dt='{{dt}}'",
                date_range
            )

        def process_and_write_table(table_name, query, date_range):
            for dt in date_range:
                data = sync_table_data(
                    mysql_cursor,
                    query.format(dt=dt),
                    table_name,
                    dt
                )
                print(f"从MySQL读取到 {len(data)} 条{table_name}数据 (dt={dt})")

                if hive_connected:
                    try:
                        # 获取表结构
                        schema = get_table_schema(table_name)
                        if not schema:
                            print(f"警告: 未找到{table_name}的表结构定义")
                            continue

                        # 处理数据
                        processed_data = []
                        for row in data:
                            processed_row = dict(row)
                            # 添加dt字段
                            processed_row['dt'] = dt
                            # 处理Decimal类型字段
                            for field in schema.fields:
                                field_name = field.name
                                if field_name in processed_row and isinstance(processed_row[field_name], Decimal):
                                    # 如果是Decimal类型，转换为float
                                    processed_row[field_name] = float(processed_row[field_name])
                            processed_data.append(processed_row)

                        # 创建DataFrame
                        df = spark.createDataFrame(processed_data, schema=schema)

                        # 写入Hive表，指定格式为hive
                        df.write.format("hive").mode('append').partitionBy('dt').saveAsTable(f"{hive_config['database']}.ods_{table_name}")
                        print(f"成功写入{len(data)}条数据到Hive表ods_{table_name} (dt={dt})")
                    except Exception as e:
                        print(f"写入Hive表失败 (dt={dt}): {e}")

        if hive_connected:
            print("数据同步完成！")
        else:
            print("MySQL数据读取完成！Hive同步已跳过。")

    except Exception as e:
        print(f"数据同步过程中出现错误: {e}")
        traceback.print_exc()
    finally:
        if mysql_conn:
            mysql_cursor.close()
            mysql_conn.close()
            print("MySQL连接已关闭")
        if spark:
            spark.stop()
            print("Spark会话已关闭")


if __name__ == "__main__":
    sync_data_to_hive()
