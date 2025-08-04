import numpy as np
import sys
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.multioutput import MultiOutputClassifier
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import classification_report, accuracy_score, f1_score, roc_auc_score
from sklearn.pipeline import Pipeline
import logging
import socket
import decimal
import joblib
from datetime import datetime
import yaml
import argparse
import os
from pyspark.sql import SparkSession, types, functions as F

# 配置日志
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 默认配置
DEFAULT_CONFIG = {
    'data_source': {
        'type': 'hive',  # 可选: 'hive', 'mysql'
        'database': 'gmall_09',
        'table_name': 'ads_wireless_entry_stats',
        'dt': '20250801'
    },
    'hive': {
        'hdfs_host': 'cdh01',
        'hdfs_port': 8020,
        'hive_metastore_port': 9083,
        'warehouse_dir': '/user/hive/warehouse'
    },
    'mysql': {
        'host': 'cdh03',
        'port': 3306,
        'database': 'gmall_09_ads',
        'user': 'root',
        'password': 'root'
    },
    'feature_engineering': {
        'numeric_features': ['visitor_count', 'new_visitor_count', 'order_buyer_count', 'order_conversion_rate', 'avg_stay_duration'],
        'categorical_features': ['time_period', 'entry_page_name'],
        'create_interaction_features': True,
        'interaction_features': [['visitor_count', 'order_buyer_count'], ['order_conversion_rate', 'avg_stay_duration']]
    },
    'label_generation': {
        'high_conversion_threshold': 0.05,
        'high_visitor_threshold': 0.75,
        'high_duration_threshold': 0.7,
        'generate_high_value_label': True
    },
    'model': {
        'type': 'random_forest',  # 可选: 'random_forest', 'gradient_boosting'
        'random_forest': {
            'n_estimators': 100,
            'max_depth': None,
            'min_samples_split': 2,
            'min_samples_leaf': 1,
            'random_state': 42
        },
        'gradient_boosting': {
            'n_estimators': 100,
            'learning_rate': 0.1,
            'max_depth': 3,
            'random_state': 42
        },
        'test_size': 0.3,
        'hyperparameter_tuning': False,
        'tuning': {
            'cv': 3,
            'scoring': 'accuracy',
            'param_grid': {
                'n_estimators': [50, 100, 200],
                'max_depth': [None, 5, 10]
            }
        }
    },
    'output': {
        'model_dir': 'models',
        'output_dir': 'output',
        'hive_table': 'ads_wireless_label_predictions',
        'save_model': True,
        'save_predictions': True,
        'prediction_format': 'csv'  # 修改默认格式为 CSV（避免 Hive 依赖）
    }
}

class LabelModelDevelopment:
    def __init__(self, config=None):
        self.config = config if config else DEFAULT_CONFIG
        self.spark = self._create_spark_session()
        self.model = None
        self.feature_transformer = None
        self.label_columns = []

    def _create_spark_session(self):
        """创建Spark会话，显式指定 Python 路径"""
        try:
            python_path = sys.executable
            logger.info(f"使用Python路径: {python_path}")

            spark = SparkSession.builder \
                .appName("LabelModelDevelopment") \
                .config("spark.sql.warehouse.dir", self.config['hive']['warehouse_dir']) \
                .config("hive.metastore.uris", f"thrift://{self.config['hive']['hdfs_host']}:{self.config['hive']['hive_metastore_port']}") \
                .config("spark.pyspark.python", python_path)  \
            .config("spark.pyspark.driver.python", python_path) \
                .enableHiveSupport() \
                .getOrCreate()
        logger.info("Spark 会话创建成功")
        return spark
    except Exception as e:
    logger.error(f"创建Spark会话失败: {str(e)}")
    raise

def load_data(self):
    """从数据源加载数据"""
    try:
        data_source = self.config['data_source']
        logger.info(f"从 {data_source['type']} 加载数据...")

        if data_source['type'] == 'hive':
            query = f"SELECT * FROM {data_source['database']}.{data_source['table_name']} WHERE dt = '{data_source['dt']}'"
            logger.info(f"执行Hive查询: {query}")
            df = self.spark.sql(query).toPandas()
        elif data_source['type'] == 'mysql':
            mysql_config = self.config['mysql']
            jdbc_url = f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
            properties = {'user': mysql_config['user'], 'password': mysql_config['password'], 'driver': 'com.mysql.jdbc.Driver'}
            df = self.spark.read.jdbc(url=jdbc_url, table=data_source['table_name'], properties=properties).toPandas()
        else:
            raise ValueError(f"不支持的数据源类型: {data_source['type']}")

        logger.info(f"成功加载数据，共 {len(df)} 行")
        return df
    except Exception as e:
        logger.error(f"加载数据失败: {str(e)}")
        raise

def preprocess_features(self, df):
    """特征预处理（修复交互特征计算问题）"""
    try:
        logger.info("开始特征预处理...")
        feature_config = self.config['feature_engineering']
        numeric_features = [f for f in feature_config['numeric_features'] if f in df.columns]
        categorical_features = [f for f in feature_config['categorical_features'] if f in df.columns]

        # 处理 Decimal 类型
        for col in df.columns:
            if df[col].dtype == 'object' and any(isinstance(x, decimal.Decimal) for x in df[col].dropna()):
                df[col] = df[col].astype(float)

        # 特征转换器
        preprocessor = ColumnTransformer(
            transformers=[
                ('num', StandardScaler(), numeric_features),
                ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
            ])
        X = preprocessor.fit_transform(df[numeric_features + categorical_features])
        self.feature_transformer = preprocessor

        # 交互特征计算（修复维度问题）
        if feature_config['create_interaction_features']:
            interaction_values = []
            num_features_indices = {f: i for i, f in enumerate(numeric_features)}

            for feature_pair in feature_config['interaction_features']:
                if all(f in numeric_features for f in feature_pair):
                    idx1, idx2 = num_features_indices[feature_pair[0]], num_features_indices[feature_pair[1]]]
                    interaction = X[:, idx1] * X[:, idx2]  # 直接按元素相乘
                    interaction_values.append(interaction.reshape(-1, 1))
                    logger.info(f"创建交互特征: {feature_pair[0]}*{feature_pair[1]}")

                    if interaction_values:
                        X = np.hstack([X] + interaction_values)

                    logger.info(f"特征矩阵形状: {X.shape}")
        return X
    except Exception as e:
        logger.error(f"特征预处理失败: {str(e)}")
        raise

def generate_labels(self, df):
    """生成标签（逻辑不变）"""
    try:
        logger.info("开始生成标签...")
        label_config = self.config['label_generation']
        self.label_columns = []
        labels = []

        if 'order_conversion_rate' in df.columns:
            high_conversion = (df['order_conversion_rate'] >= label_config['high_conversion_threshold']).astype(int)
            labels.append(high_conversion)
            self.label_columns.append('high_conversion')

        if 'visitor_count' in df.columns:
            quantile = df['visitor_count'].quantile(label_config['high_visitor_threshold'])
            high_visitor = (df['visitor_count'] >= quantile).astype(int)
            labels.append(high_visitor)
            self.label_columns.append('high_visitor')

        if label_config['generate_high_value_label'] and 'avg_stay_duration' in df.columns:
            quantile = df['avg_stay_duration'].quantile(label_config['high_duration_threshold'])
            high_duration = (df['avg_stay_duration'] >= quantile).astype(int)
            labels.append(high_duration)
            self.label_columns.append('high_duration')

        y = np.column_stack(labels)
        logger.info(f"生成 {y.shape[1]} 个标签: {self.label_columns}")
        return y
    except Exception as e:
        logger.error(f"标签生成失败: {str(e)}")
        raise

def train_model(self, X, y):
    """训练模型（逻辑不变）"""
    try:
        logger.info("开始训练模型...")
        model_config = self.config['model']
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=model_config['test_size'], random_state=42)

        if model_config['type'] == 'random_forest':
            base_model = RandomForestClassifier(**model_config['random_forest'])
        else:
            base_model = GradientBoostingClassifier(**model_config['gradient_boosting'])

        model = MultiOutputClassifier(base_model, n_jobs=-1)
        model.fit(X_train, y_train)

        # 评估模型
        logger.info("模型评估结果:")
        y_pred = model.predict(X_test)
        for i, label in enumerate(self.label_columns):
            logger.info(f"标签 {label}:")
            logger.info(classification_report(y_test[:, i], y_pred[:, i], zero_division=1))

        self.model = model
        return model
    except Exception as e:
        logger.error(f"模型训练失败: {str(e)}")
        raise

def save_model(self):
    """保存模型到本地（逻辑不变）"""
    try:
        if not self.model:
            raise ValueError("模型未训练")

        model_dir = self.config['output']['model_dir']
        os.makedirs(model_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        joblib.dump(self.model, os.path.join(model_dir, f"label_model_{timestamp}.joblib"))
        if self.feature_transformer:
            joblib.dump(self.feature_transformer, os.path.join(model_dir, f"feature_transformer_{timestamp}.joblib"))

        logger.info(f"模型保存至: {model_dir}")
    except Exception as e:
        logger.error(f"保存模型失败: {str(e)}")
        raise

def save_predictions(self, df, X):
    """保存预测结果（优先 CSV，Hive 失败时自动回退）"""
    try:
        if not self.model:
            raise ValueError("模型未训练")

        output_config = self.config['output']
        result_df = df.copy()

        # 生成预测结果
        y_pred = self.model.predict(X)
        for i, label in enumerate(self.label_columns):
            result_df[f"{label}_pred"] = y_pred[:, i]

        # 保存逻辑
        if output_config['prediction_format'] == 'hive':
            try:
                spark_df = self.spark.createDataFrame(result_df)
                spark_df.write.mode('overwrite').saveAsTable(output_config['hive_table'])
                logger.info(f"预测结果保存至 Hive 表: {output_config['hive_table']}")
            except Exception as hive_error:
                logger.warning(f"Hive 保存失败，回退到 CSV: {str(hive_error)}")
                self._save_to_csv(result_df)
        else:
            self._save_to_csv(result_df)

    except Exception as e:
        logger.error(f"保存预测结果失败: {str(e)}")
        self._save_to_csv(result_df)  # 强制回退到 CSV

def _save_to_csv(self, result_df):
    """辅助方法：保存为 CSV"""
    output_dir = self.config['output']['output_dir']
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, f"predictions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    result_df.to_csv(csv_path, index=False)
    logger.info(f"预测结果已保存为 CSV: {csv_path}")

def run_pipeline(self):
    """完整流程"""
    try:
        df = self.load_data()
        X = self.preprocess_features(df)
        y = self.generate_labels(df)
        self.train_model(X, y)
        self.save_model()
        self.save_predictions(df, X)
        logger.info("流程执行成功")
    except Exception as e:
        logger.error(f"流程执行失败: {str(e)}")
        raise

def main():
    """主函数（逻辑不变）"""
    parser = argparse.ArgumentParser(description='标签模型开发工具')
    parser.add_argument('--table', type=str, help='数据源表名')
    parser.add_argument('--database', type=str, help='数据库名')
    parser.add_argument('--dt', type=str, help='日期分区')
    parser.add_argument('--source', type=str, choices=['hive', 'mysql'], help='数据源类型')
    parser.add_argument('--config', type=str, help='配置文件路径')
    args = parser.parse_args()

    try:
        config = DEFAULT_CONFIG.copy()
        if args.config and os.path.exists(args.config):
            with open(args.config, 'r', encoding='utf-8') as f:
                config.update(yaml.safe_load(f))

        # 覆盖命令行参数
        if args.table: config['data_source']['table_name'] = args.table
        if args.database: config['data_source']['database'] = args.database
        if args.dt: config['data_source']['dt'] = args.dt
        if args.source: config['data_source']['type'] = args.source

        label_model = LabelModelDevelopment(config)
        label_model.run_pipeline()
    except Exception as e:
        logger.error(f"主函数错误: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()