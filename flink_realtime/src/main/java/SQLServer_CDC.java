import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * SQL Server CDC 数据同步程序
 * 基于 Flink CDC 捕获 SQL Server 表的变更数据（insert/update/delete）
 */
public class SQLServer_CDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties debeziumProperties = new Properties();
//        debeziumProperties.put("snapshot.mode","schema_only");
        debeziumProperties.put("snapshot.mode","initial");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        DebeziumSourceFunction<String> sqlServerSource = SqlServerSource.<String>builder()
                .hostname("cdh02")
                .port(1433)
                .username("sa")
                .password("root1,./")
                .database("test")
                .tableList("dbo.test_data")
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(sqlServerSource, "_transaction_log_source1");
        dataStreamSource.print().setParallelism(1);
        env.execute("sqlserver_data_test");
    }
}