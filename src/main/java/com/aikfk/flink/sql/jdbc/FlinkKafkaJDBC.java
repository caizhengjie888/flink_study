package com.aikfk.flink.sql.jdbc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class FlinkKafkaJDBC {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env , settings);

        String catalogName = "flink_hive";
        String hiveDataBase = "flink";
        String hiveConfDir = "/Users/caizhengjie/Desktop/hive-conf";

        HiveCatalog hiveCatalog =
                new HiveCatalog(catalogName,hiveDataBase,hiveConfDir);

        tableEnvironment.registerCatalog(catalogName , hiveCatalog);
        tableEnvironment.useCatalog(catalogName);
        String kafkaTable = "kafka_person";
        String kafkaDropsql = "DROP TABLE IF EXISTS kafka_person";
        String kafakTable_sql
                = "CREATE TABLE kafka_person (\n" +
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
        tableEnvironment.executeSql(kafkaDropsql);
        tableEnvironment.executeSql(kafakTable_sql);

        // register a MySQL table 'person' in Flink SQL
        String mysqlTable_sql =
                "CREATE TABLE mysql_person (\n" +
                        "  user_id String,\n" +
                        "  user_name String,\n" +
                        "  age INT\n" +
                        ") WITH (\n" +
                        "   'connector' = 'jdbc',\n" +
                        "   'url' = 'jdbc:mysql://bigdata-pro-m07:3306/flink',\n" +
                        "   'table-name' = 'person',\n" +
                        "   'username' = 'root',\n" +
                        "   'password' = '199911'\n" +
                        ")";
        String mysqlDropsql = "DROP TABLE IF EXISTS mysql_person";
        tableEnvironment.executeSql(mysqlDropsql);
        tableEnvironment.executeSql(mysqlTable_sql);

        // write data into the JDBC table from the other table "kafka_person"
        tableEnvironment.executeSql("INSERT INTO mysql_person\n" +
                "SELECT user_id, user_name, age FROM kafka_person");

        env.execute("kafka");

    }
}
