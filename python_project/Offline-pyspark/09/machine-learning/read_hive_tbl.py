import os

import numpy as np
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
        'table_name': 'ads_wireless_entry_stats',  # 更新默认表名
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
        'high_conversion_threshold': 0.05,  # 调整阈值适应新数据
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
        'prediction_format': 'hive'
    }
}

class LabelModelDevelopment:
    def __init__(self, config=None):
        """初始化标签模型开发类
        参数:
            config: 配置字典，如果为None则使用默认配置
        """
        self.config = config if config else DEFAULT_CONFIG
        self.spark = self._create_spark_session()
        self.model = None
        self.feature_transformer = None
        self.label_columns = []

    def _create_spark_session(self):
        """创建Spark会话"""
        try:
            spark = SparkSession.builder \
                .appName("LabelModelDevelopment") \
                .config("spark.sql.warehouse.dir", self.config['hive']['warehouse_dir']) \
                .config("hive.metastore.uris", f"thrift://{self.config['hive']['hdfs_host']}:{self.config['hive']['hive_metastore_port']}") \
                .enableHiveSupport() \
                .getOrCreate()
            logger.info("成功创建Spark会话")
            return spark
        except Exception as e:
            logger.error(f"创建Spark会话失败: {str(e)}")
            raise

    def load_data(self):
        """从数据源加载数据
        返回:
            pandas DataFrame
        """
        try:
            data_source = self.config['data_source']
            logger.info(f"从{data_source['type']}加载数据...")

            if data_source['type'] == 'hive':
                # 从Hive加载数据
                database = data_source['database']
                table_name = data_source['table_name']
                dt = data_source['dt']

                # 检查是否有自定义查询
                if 'query' in data_source and data_source['query']:
                    query = data_source['query']
                else:
                    query = f"SELECT * FROM {database}.{table_name} WHERE dt = '{dt}'"
                logger.info(f"执行Hive查询: {query}")
                df = self.spark.sql(query).toPandas()
            elif data_source['type'] == 'mysql':
                # 从MySQL加载数据
                mysql_config = self.config['mysql']
                jdbc_url = f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
                properties = {
                    'user': mysql_config['user'],
                    'password': mysql_config['password'],
                    'driver': 'com.mysql.jdbc.Driver'
                }
                table_name = data_source['table_name']
                logger.info(f"从MySQL表{table_name}加载数据")
                df = self.spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties).toPandas()
            else:
                raise ValueError(f"不支持的数据源类型: {data_source['type']}")

            logger.info(f"成功加载数据，共{len(df)}行")
            return df
        except Exception as e:
            logger.error(f"加载数据失败: {str(e)}")
            raise

    def preprocess_features(self, df):
        """特征预处理
        参数:
            df: 输入DataFrame
        返回:
            预处理后的特征矩阵
        """
        try:
            logger.info("开始特征预处理...")
            feature_config = self.config['feature_engineering']

            # 提取特征
            numeric_features = feature_config['numeric_features']
            categorical_features = feature_config['categorical_features']

            # 检查特征是否存在
            for feature in numeric_features + categorical_features:
                if feature not in df.columns:
                    logger.warning(f"特征{feature}不存在于数据中，将被忽略")

            # 过滤存在的特征
            numeric_features = [f for f in numeric_features if f in df.columns]
            categorical_features = [f for f in categorical_features if f in df.columns]

            logger.info(f"使用数值特征: {numeric_features}")
            logger.info(f"使用类别特征: {categorical_features}")

            # 处理decimal类型
            for col in df.columns:
                if df[col].dtype == 'object':
                    # 检查是否包含decimal.Decimal类型
                    if any(isinstance(x, decimal.Decimal) for x in df[col].dropna()):
                        logger.info(f"将列{col}从decimal.Decimal转换为float")
                        df[col] = df[col].apply(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)

            # 创建特征转换器
            preprocessor = ColumnTransformer(
                transformers=[
                    ('num', StandardScaler(), numeric_features),
                    ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
                ])

            # 拟合和转换特征
            X = preprocessor.fit_transform(df[numeric_features + categorical_features])
            self.feature_transformer = preprocessor

            # 创建交互特征
            if feature_config['create_interaction_features'] and len(numeric_features) >= 2:
                logger.info("创建交互特征...")
                interaction_features = feature_config['interaction_features']
                interaction_values = []

                # 获取数值特征在X中的索引
                num_features_count = len(numeric_features)
                num_features_indices = list(range(num_features_count))
                feature_to_index = {f: i for i, f in enumerate(numeric_features)}

                for features in interaction_features:
                    if all(f in numeric_features for f in features):
                        # 从X中提取标准化后的数值特征
                        feature_indices = [feature_to_index[f] for f in features]
                        scaled_features = X[:, feature_indices]

                        # 检查并处理数组维度
                        logger.info(f"交互特征{features}的scaled_features初始维度: {scaled_features.ndim}")
                        if scaled_features.ndim == 0:
                            logger.warning(f"跳过交互特征{features}，因为提取的特征是0维的")
                            continue
                        elif scaled_features.ndim == 1:
                            scaled_features = scaled_features.reshape(-1, 1)
                            logger.info(f"将交互特征{features}的scaled_features重塑为2维")

                        # 计算交互特征 (乘积)
                        try:
                            interaction = np.prod(scaled_features, axis=1)
                            interaction_values.append(interaction)
                            logger.info(f"创建交互特征: {features[0]}*{features[1]}")
                        except Exception as e:
                            logger.error(f"计算交互特征{features}时出错: {str(e)}")
                            logger.info(f"scaled_features形状: {scaled_features.shape}")
                    else:
                        logger.warning(f"跳过交互特征: {features}，因为某些特征不存在或不是数值特征")

                if interaction_values:
                    interaction_values = np.array(interaction_values).T
                    X = np.hstack((X, interaction_values))

            logger.info(f"特征预处理完成，特征矩阵形状: {X.shape}")
            return X
        except Exception as e:
            logger.error(f"特征预处理失败: {str(e)}")
            raise

    def generate_labels(self, df):
        """生成标签
        参数:
            df: 输入DataFrame
        返回:
            标签矩阵
        """
        try:
            logger.info("开始生成标签...")
            label_config = self.config['label_generation']
            self.label_columns = []
            labels = []

            # 生成高转化率标签
            if 'order_conversion_rate' in df.columns:
                threshold = label_config['high_conversion_threshold']
                high_conversion = (df['order_conversion_rate'] >= threshold).astype(int)
                labels.append(high_conversion)
                self.label_columns.append('high_conversion')
                logger.info(f"生成高转化率标签，阈值: {threshold}")
            else:
                logger.warning("order_conversion_rate列不存在，无法生成高转化率标签")

            # 生成高访客标签
            if 'visitor_count' in df.columns:
                threshold = label_config['high_visitor_threshold']
                # 计算分位数
                quantile = df['visitor_count'].quantile(threshold)
                high_visitor = (df['visitor_count'] >= quantile).astype(int)
                labels.append(high_visitor)
                self.label_columns.append('high_visitor')
                logger.info(f"生成高访客标签，分位数阈值: {threshold}, 值: {quantile}")
            else:
                logger.warning("visitor_count列不存在，无法生成高访客标签")

            # 生成高停留时长标签
            if label_config['generate_high_value_label'] and 'avg_stay_duration' in df.columns:
                threshold = label_config['high_duration_threshold']
                # 计算分位数
                quantile = df['avg_stay_duration'].quantile(threshold)
                high_duration = (df['avg_stay_duration'] >= quantile).astype(int)
                labels.append(high_duration)
                self.label_columns.append('high_duration')
                logger.info(f"生成高停留时长标签，分位数阈值: {threshold}, 值: {quantile}")
            elif label_config['generate_high_value_label']:
                logger.warning("avg_stay_duration列不存在，无法生成高停留时长标签")

            if not labels:
                raise ValueError("无法生成任何标签，请检查数据是否包含必要的列")

            # 构建标签矩阵
            y = np.column_stack(labels)
            logger.info(f"标签生成完成，标签矩阵形状: {y.shape}，标签列: {self.label_columns}")
            return y
        except Exception as e:
            logger.error(f"标签生成失败: {str(e)}")
            raise

    def train_model(self, X, y):
        """训练模型
        参数:
            X: 特征矩阵
            y: 标签矩阵
        返回:
            训练好的模型
        """
        try:
            logger.info("开始训练模型...")
            model_config = self.config['model']

            # 检查数据量，如果太小则调整测试集比例
            total_samples = X.shape[0]
            if total_samples < 10:
                logger.warning(f"数据量较少({total_samples}行)，将测试集比例调整为0.1")
                test_size = 0.1
            else:
                test_size = model_config['test_size']

            # 分割训练集和测试集
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=test_size, random_state=42
            )
            logger.info(f"训练集大小: {X_train.shape[0]}, 测试集大小: {X_test.shape[0]}")

            # 选择基础模型
            if model_config['type'] == 'random_forest':
                base_model = RandomForestClassifier(**model_config['random_forest'])
            elif model_config['type'] == 'gradient_boosting':
                base_model = GradientBoostingClassifier(**model_config['gradient_boosting'])
            else:
                raise ValueError(f"不支持的模型类型: {model_config['type']}")

            # 创建多输出分类器
            model = MultiOutputClassifier(base_model, n_jobs=-1)

            # 超参数调优
            if model_config['hyperparameter_tuning'] and X_train.shape[0] >= 5:
                logger.info("开始超参数调优...")
                param_grid = model_config['tuning']['param_grid']
                # 调整参数网格以适应多输出分类器
                tuned_param_grid = {f'estimator__{k}': v for k, v in param_grid.items()}

                grid_search = GridSearchCV(
                    estimator=model,
                    param_grid=tuned_param_grid,
                    cv=min(model_config['tuning']['cv'], X_train.shape[0]//2),  # 确保交叉验证折数不超过训练集大小的一半
                    scoring=model_config['tuning']['scoring'],
                    n_jobs=-1
                )

                grid_search.fit(X_train, y_train)
                logger.info(f"最佳参数: {grid_search.best_params_}")
                model = grid_search.best_estimator_
            elif model_config['hyperparameter_tuning'] and X_train.shape[0] < 5:
                logger.warning("训练集太小，无法进行超参数调优")
                # 直接训练模型
                model.fit(X_train, y_train)
            else:
                # 直接训练模型
                model.fit(X_train, y_train)

            # 评估模型
            logger.info("评估模型性能...")
            y_pred = model.predict(X_test)
            y_pred_proba = model.predict_proba(X_test) if hasattr(model, 'predict_proba') else None

            # 计算评估指标
            for i, label in enumerate(self.label_columns):
                logger.info(f"标签 {label} 评估结果:")
                # 设置zero_division=1以避免警告
                logger.info(f"准确率: {accuracy_score(y_test[:, i], y_pred[:, i]):.4f}")
                logger.info(f"F1分数: {f1_score(y_test[:, i], y_pred[:, i], average='macro', zero_division=1):.4f}")
                if y_pred_proba:
                    try:
                        logger.info(f"ROC-AUC: {roc_auc_score(y_test[:, i], y_pred_proba[i][:, 1]):.4f}")
                    except:
                        logger.warning(f"无法计算标签 {label} 的ROC-AUC")
                # 设置zero_division=1以避免警告
                logger.info(classification_report(y_test[:, i], y_pred[:, i], zero_division=1))

            # 特征重要性
            if hasattr(model.estimators_[0], 'feature_importances_'):
                feature_importance = np.mean([estimator.feature_importances_ for estimator in model.estimators_], axis=0)
                logger.info("特征重要性:")
                for i, importance in enumerate(feature_importance):
                    logger.info(f"特征 {i}: {importance:.4f}")

            self.model = model
            logger.info("模型训练完成")
            return model
        except Exception as e:
            logger.error(f"模型训练失败: {str(e)}")
            raise

    def save_model(self):
        """保存模型"""
        try:
            if not self.model:
                raise ValueError("模型尚未训练，无法保存")

            output_config = self.config['output']
            if output_config['save_model']:
                model_dir = output_config['model_dir']
                os.makedirs(model_dir, exist_ok=True)

                # 保存模型
                model_path = os.path.join(model_dir, f"label_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}.joblib")
                joblib.dump(self.model, model_path)
                logger.info(f"模型保存至: {model_path}")

                # 保存特征转换器
                if self.feature_transformer:
                    transformer_path = os.path.join(model_dir, f"feature_transformer_{datetime.now().strftime('%Y%m%d_%H%M%S')}.joblib")
                    joblib.dump(self.feature_transformer, transformer_path)
                    logger.info(f"特征转换器保存至: {transformer_path}")
        except Exception as e:
            logger.error(f"保存模型失败: {str(e)}")
            raise

    def save_predictions(self, df, X):
        """保存预测结果
        参数:
            df: 原始数据DataFrame
            X: 预处理后的特征矩阵
        """
        try:
            if not self.model:
                raise ValueError("模型尚未训练，无法进行预测")

            output_config = self.config['output']
            if output_config['save_predictions']:
                # 进行预测
                logger.info("生成预测结果...")
                y_pred = self.model.predict(X)
                y_pred_proba = self.model.predict_proba(X) if hasattr(self.model, 'predict_proba') else None

                # 创建结果DataFrame
                result_df = df.copy()

                # 添加预测标签
                for i, label in enumerate(self.label_columns):
                    result_df[f"{label}_pred"] = y_pred[:, i]
                    if y_pred_proba:
                        try:
                            # 检查概率数组的列数
                            if y_pred_proba[i].shape[1] > 1:
                                result_df[f"{label}_prob"] = y_pred_proba[i][:, 1]
                            else:
                                # 如果只有一列，可能是因为标签只有一个类别
                                logger.warning(f"标签 {label} 的预测概率只有一列，将概率设置为1.0")
                                result_df[f"{label}_prob"] = 1.0
                        except Exception as e:
                            logger.error(f"添加标签 {label} 的概率时出错: {str(e)}")

                # 添加预测时间
                result_df['prediction_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # 保存结果
                prediction_format = output_config['prediction_format']
                if prediction_format == 'hive':
                    # 保存到Hive
                    hive_table = output_config['hive_table']
                    # 创建Spark DataFrame
                    spark_df = self.spark.createDataFrame(result_df)

                    # 定义表结构，处理可能的Decimal类型
                    schema = types.StructType()
                    for field in spark_df.schema.fields:
                        if field.dataType.simpleString().startswith('decimal'):
                            schema.add(field.name, types.DoubleType())
                        else:
                            schema.add(field.name, field.dataType)

                    # 应用新schema
                    spark_df = self.spark.createDataFrame(spark_df.rdd, schema)

                    # 保存到Hive
                    spark_df.write.mode('overwrite').saveAsTable(hive_table)
                    logger.info(f"预测结果保存至Hive表: {hive_table}")
                elif prediction_format == 'csv':
                    # 保存到CSV
                    output_dir = output_config['output_dir']
                    os.makedirs(output_dir, exist_ok=True)
                    output_path = os.path.join(output_dir, f"predictions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                    result_df.to_csv(output_path, index=False)
                    logger.info(f"预测结果保存至CSV文件: {output_path}")
                else:
                    raise ValueError(f"不支持的预测结果格式: {prediction_format}")
        except Exception as e:
            logger.error(f"保存预测结果失败: {str(e)}")
            raise

    def run_pipeline(self):
        """运行完整流程"""
        try:
            # 加载数据
            df = self.load_data()

            # 特征预处理
            X = self.preprocess_features(df)

            # 生成标签
            y = self.generate_labels(df)

            # 训练模型
            self.train_model(X, y)

            # 保存模型
            self.save_model()

            # 保存预测结果
            self.save_predictions(df, X)

            logger.info("标签模型开发流程运行完成")
        except Exception as e:
            logger.error(f"流程运行失败: {str(e)}")
            raise


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='标签模型开发工具')
    parser.add_argument('--table', type=str, help='数据源表名 (默认: ads_user_segment)')
    parser.add_argument('--database', type=str, help='数据库名 (默认: gmall_09)')
    parser.add_argument('--dt', type=str, help='日期分区 (默认: 20250801)')
    parser.add_argument('--source', type=str, choices=['hive', 'mysql'], help='数据源类型 (默认: hive)')
    parser.add_argument('--config', type=str, help='配置文件路径 (默认: 使用内置配置)')
    args = parser.parse_args()

    # 加载配置
    try:
        if args.config and os.path.exists(args.config):
            with open(args.config, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
            logger.info(f"成功加载配置文件: {args.config}")
        else:
            config = DEFAULT_CONFIG.copy()
            logger.info("使用默认配置")
    except Exception as e:
        logger.error(f"加载配置文件失败: {str(e)}")
        config = DEFAULT_CONFIG.copy()
        logger.info("使用默认配置")

    # 更新命令行参数到配置
    if args.table:
        config['data_source']['table_name'] = args.table
        logger.info(f"设置表名为: {args.table}")
    if args.database:
        config['data_source']['database'] = args.database
        logger.info(f"设置数据库名为: {args.database}")
    if args.dt:
        config['data_source']['dt'] = args.dt
        logger.info(f"设置日期分区为: {args.dt}")
    if args.source:
        config['data_source']['type'] = args.source
        logger.info(f"设置数据源类型为: {args.source}")

    # 初始化标签模型开发类
    label_model = LabelModelDevelopment(config)

    # 运行完整流程
    logger.info("开始运行标签模型开发流程...")
    label_model.run_pipeline()


if __name__ == '__main__':
    main()