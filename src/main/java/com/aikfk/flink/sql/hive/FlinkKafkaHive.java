package com.aikfk.flink.sql.hive;

import com.aikfk.flink.sql.CommonSQL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class FlinkKafkaHive {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env , settings);

        env.enableCheckpointing(5000);

        HiveCatalog hiveCatalog =
                new HiveCatalog(
                        CommonSQL.hiveCatalog_name,
                        CommonSQL.hiveDatabase_name,
                        CommonSQL.hiveConfDir,
                        CommonSQL.version
                        );
        tableEnvironment.registerCatalog(CommonSQL.hiveCatalog_name,hiveCatalog);
        tableEnvironment.useCatalog(CommonSQL.hiveCatalog_name);

        tableEnvironment.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnvironment.executeSql(CommonSQL.user_product_kafka_drop);
        tableEnvironment.executeSql(CommonSQL.user_product_kafka_create);

        tableEnvironment.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnvironment.executeSql(CommonSQL.user_product_hive_drop);
        tableEnvironment.executeSql(CommonSQL.user_product_hive_create);

        tableEnvironment.executeSql(CommonSQL.user_product_kafka_insert_hive).print();

        env.execute();
    }
}
