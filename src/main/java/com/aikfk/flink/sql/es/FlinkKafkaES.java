package com.aikfk.flink.sql.es;

import com.aikfk.flink.sql.CommonSQL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class FlinkKafkaES {
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

        String user_product_kafka_create =
                "CREATE TABLE user_product_kafka (\n" +
                        " row_key STRING," +
                        " user_id STRING," +
                        " product_id STRING," +
                        " click_count INT " +
                        ") WITH (" +
                        " 'connector' = 'kafka'," +
                        " 'topic' = 'kfk'," +
                        " 'properties.bootstrap.servers' = 'bigdata-pro-m07:9092'," +
                        " 'properties.group.id' = 'test1'," +
                        " 'format' = 'json'," +
                        " 'scan.startup.mode' = 'latest-offset'" +
                        ")";
        tableEnvironment.executeSql("DROP TABLE IF EXISTS user_product_kafka");
        tableEnvironment.executeSql(user_product_kafka_create);


        tableEnvironment.executeSql("DROP TABLE IF EXISTS user_product_es");

        String user_product_es_create =
                        "CREATE TABLE user_product_es (\n" +
                        "  row_key STRING,\n" +
                        "  user_id STRING,\n" +
                        "  product_id STRING,\n" +
                        "  click_count INT,\n" +
                        "  PRIMARY KEY (row_key) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "  'connector' = 'elasticsearch-6',\n" +
                        "  'hosts' = 'http://bigdata-pro-m07:9200',\n" +
                        "  'document-type' = 'user',\n" +

                        "  'index' = 'user_product_es'\n" +

                        " )";
        tableEnvironment.executeSql(user_product_es_create);

        tableEnvironment.executeSql(
                "INSERT INTO user_product_es\n" +
                "SELECT row_key,user_id, product_id, click_count FROM user_product_kafka").print();

        env.execute();


    }
}
