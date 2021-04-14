package com.aikfk.flink.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/4/5 10:32 下午
 */
public class ProcessTimeSQL {
    public static void main(String[] args) throws Exception {
        // 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.创建TableEnvironment(Blink planner)
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env , settings);

        // 3.文件path
        String filePath = "/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/dept.csv";

        // 4.DDL-- 声明一个额外的列作为处理时间属性
        String ddl = "create table dept (\n" +
                " dept_id STRING,\n" +
                " dept_name STRING,\n" +
                " user_action_time AS PROCTIME()\n" +
                ") WITH (\n" +
                " 'connector.type' = 'filesystem',\n" +
                " 'connector.path' = '"+filePath+"',\n" +
                " 'format.type' = 'csv'\n" +
                ")";

        // 5.创建一个带processtime字段的表
        tableEnvironment.executeSql(ddl);

        // 6.通过SQL对表的查询，生成结果表
        Table table = tableEnvironment.sqlQuery("select * from dept");

        // 7.将table表转换为DataStream
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnvironment.toRetractStream(table, Row.class);
        retractStream.print();
        env.execute();

        /**
         * 5> (true,100,技术部,2021-04-08T08:06:49.792)
         * 15> (true,400,采购部,2021-04-08T08:06:49.797)
         * 8> (true,200,市场部,2021-04-08T08:06:49.817)
         * 11> (true,300,营销部,2021-04-08T08:06:49.818)
         */
    }
}
