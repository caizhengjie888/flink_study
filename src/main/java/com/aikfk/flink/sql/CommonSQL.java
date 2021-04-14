package com.aikfk.flink.sql;

public class CommonSQL {

    public static final String hiveCatalog_name = "flink_udata";
    public static final String hiveDatabase_name = "flink";
    public static final String hiveConfDir = "/Users/caizhengjie/Desktop/hive-conf";
    public static final String version = "2.3.4";


    public static final String user_product_hive_create = "CREATE  TABLE  user_product_hive (\n" +
            "  user_id STRING,\n" +
            "  product_id STRING,\n" +
            "  click_count INT" +
            ") partitioned by (dt string,hr string) " +
            "stored as PARQUET " +
            "TBLPROPERTIES (\n" +
            "  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',\n" +
            "  'sink.partition-commit.delay'='5 s',\n" +
            "  'sink.partition-commit.trigger'='partition-time',\n" +
            "  'sink.partition-commit.policy.kind'='metastore,success-file'" +
            ")";

    public static final String user_product_kafka_create =
            "CREATE TABLE user_product_kafka (\n" +
            " user_id STRING," +
            " product_id STRING," +
            " click_count INT ," +
            " ts bigint ," +
            " r_t AS TO_TIMESTAMP(FROM_UNIXTIME(ts,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss'),\n" +
            " WATERMARK FOR r_t AS r_t - INTERVAL '5' SECOND " +
            ") WITH (" +
            " 'connector' = 'kafka'," +
            " 'topic' = 'kfk'," +
            " 'properties.bootstrap.servers' = 'bigdata-pro-m07:9092'," +
            " 'properties.group.id' = 'test1'," +
            " 'format' = 'json'," +
            " 'scan.startup.mode' = 'latest-offset'" +
            ")";

    public static final String user_product_kafka_drop ="DROP TABLE IF EXISTS user_product_kafka";
    public static final String user_product_hive_drop ="DROP TABLE IF EXISTS user_product_hive";

    public static final String user_product_kafka_insert_hive =
            "insert into user_product_hive SELECT user_id, product_id, click_count, " +
            " DATE_FORMAT(r_t, 'yyyy-MM-dd'), DATE_FORMAT(r_t, 'HH') FROM user_product_kafka";

}
