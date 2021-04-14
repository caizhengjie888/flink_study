package com.aikfk.flink.sql.kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.table.catalog.hive.HiveCatalog;


/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/4/10 12:53 下午
 */
public class FlinkConnectKafkaDDL {
    public static void main(String[] args) throws Exception {

        // 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.创建TableEnvironment(Blink planner)
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env , settings);

        String catalogName = "flink_hive";
        String hiveDataBase = "flink";
        String hiveConfDir = "/Users/caizhengjie/Desktop/hive-conf";

        // Catalog
        HiveCatalog hiveCatalog =
                new HiveCatalog(catalogName,hiveDataBase,hiveConfDir);

        tableEnvironment.registerCatalog(catalogName , hiveCatalog);
        tableEnvironment.useCatalog(catalogName);

        // DDL，根据kafka数据源创建表
        String kafkaTable = "person";
        String dropsql = "DROP TABLE IF EXISTS "+kafkaTable;
        String sql
                = "CREATE TABLE "+kafkaTable+" (\n" +
                "    user_id String,\n" +
                "    user_name String,\n" +
                "    age Int\n" +
                ") WITH (\n" +
                "   'connector.type' = 'kafka',\n" +
                "   'connector.version' = 'universal',\n" +
                "   'connector.topic' = 'kfk',\n" +
                "   'connector.properties.bootstrap.servers' = 'bigdata-pro-m07:9092',\n" +
                "   'format.type' = 'csv',\n" +
                "   'update-mode' = 'append'\n" +
                ")";
        tableEnvironment.executeSql(dropsql);
        tableEnvironment.executeSql(sql);

        Table table = tableEnvironment.sqlQuery("select * from person");

        tableEnvironment.toAppendStream(table , Row.class).print();

        env.execute("kafka");
    }
}
