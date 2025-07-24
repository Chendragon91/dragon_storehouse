import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SQLServer_CDC {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 配置检查点（生产环境建议开启，保证数据一致性）
        env.enableCheckpointing(60000); // 每60秒触发一次检查点
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 精确一次语义
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000); // 检查点最小间隔30秒
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // 最多同时进行1个检查点
        // 配置状态后端（根据实际环境调整路径，本地测试可用file://，集群可用hdfs://）

        // 3. 配置失败重启策略（增强容错性）
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 最大重启次数
                TimeUnit.SECONDS.toMillis(10) // 重启间隔10秒
        ));

        // 4. 配置Debezium参数（增加必要的CDC配置）
        Properties debeziumProperties = new Properties();
        // 快照模式：初始快照后只捕获增量变更（生产环境推荐）
        debeziumProperties.put("snapshot.mode", "initial");
        // 只记录监控表的DDL（减少非必要日志）
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        // 数据库历史记录存储方式（文件系统，根据实际环境调整路径）
        debeziumProperties.put("database.history", "io.debezium.relational.history.FileDatabaseHistory");
        debeziumProperties.put("database.history.file.filename", "/tmp/flink-sqlserver-cdc-history.dat");
        // 心跳检测（防止长事务导致连接超时）
        debeziumProperties.put("heartbeat.interval.ms", "30000");

        // 5. 构建SQL Server CDC源（建议将配置参数通过外部传入，此处简化为硬编码）
        try {
            DebeziumSourceFunction<String> sqlServerSource = SqlServerSource.<String>builder()
                    .hostname("192.168.142.129")      // SQL Server主机地址
                    .port(1433)                     // 端口
                    .username("sa")                 // 用户名
                    .password("root1,./")          // 密码（生产环境建议加密存储）
                    .database("test")               // 数据库名
                    .tableList("dbo.test_data")      // 监控表（格式：schema.table，多表用逗号分隔）
                    .startupOptions(StartupOptions.initial())  // 启动策略：初始快照+增量
                    .debeziumProperties(debeziumProperties)
                    .deserializer(new JsonDebeziumDeserializationSchema())  // 反序列化为JSON
                    .build();

            // 6. 添加源并处理数据（增加异常捕获）
            DataStreamSource<String> dataStreamSource = env.addSource(
                    sqlServerSource,
                    "_transaction_log_source1"  // 源名称，便于监控
            );

            // 打印数据（生产环境可替换为写入Kafka/Hive等目标端）
            dataStreamSource
                    .name("cdc-data-print")
                    .setParallelism(1)  // 单并行度避免输出乱序
                    .print();

            // 7. 执行任务
            env.execute("sqlserver-cdc-test");

        } catch (Exception e) {
            throw new RuntimeException("任务初始化异常", e);
        }
    }
}