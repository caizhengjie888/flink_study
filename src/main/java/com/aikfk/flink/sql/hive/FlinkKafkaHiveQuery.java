package com.aikfk.flink.sql.hive;

import com.aikfk.flink.sql.CommonSQL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

public class FlinkKafkaHiveQuery {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env , settings);


        HiveCatalog hiveCatalog =
                new HiveCatalog(
                        CommonSQL.hiveCatalog_name,
                        CommonSQL.hiveDatabase_name,
                        CommonSQL.hiveConfDir,
                        CommonSQL.version
                        );
        tableEnvironment.registerCatalog(CommonSQL.hiveCatalog_name,hiveCatalog);
        tableEnvironment.useCatalog(CommonSQL.hiveCatalog_name);

        Table table = tableEnvironment.sqlQuery("select * from u_data limit 5");
        tableEnvironment.toRetractStream(table , Row.class).print();

        env.execute();


    }
}
