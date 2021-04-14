package com.aikfk.flink.sql;

import com.aikfk.flink.sql.pojo.WC;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/4/5 10:32 下午
 */
public class WordCountStreamSQL {
    public static void main(String[] args) throws Exception {
        // 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.创建TableEnvironment(Blink planner)
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env , settings);

        // 3.batch数据源
        DataStream<WC> dataStream = env.fromElements(
                new WC("java", 1),
                new WC("spark", 1),
                new WC("hive", 1),
                new WC("hbase", 1),
                new WC("hbase", 1),
                new WC("hadoop", 1),
                new WC("java", 1));


        // 将dataStream流转换为table表
        Table wordcount = tableEnvironment.fromDataStream(dataStream,$("wordName"), $("freq"));

        // 4.将dataStream转换为视图
        tableEnvironment.createTemporaryView("wordcount" ,dataStream,$("wordName"), $("freq"));

        // 5.通过SQL对表的查询，生成结果表
        Table tableResult = tableEnvironment.sqlQuery("select wordName,sum(freq) as freq " +
                "from wordcount group by wordName having sum(freq) > 1" +
                "union all select wordName,sum(freq) as freq from "+wordcount+" group by wordName having sum(freq) > 1");

        // 6.将table表转换为DataStream
        DataStream<Tuple2<Boolean, WC>> retractStream = tableEnvironment.toRetractStream(tableResult, WC.class);
        retractStream.print();
        env.execute();


        /**
         * 15> (true,WC{wordName='java', freq=2})
         * 15> (true,WC{wordName='java', freq=2})
         * 7> (true,WC{wordName='hbase', freq=2})
         * 7> (true,WC{wordName='hbase', freq=2})
         */
    }
}
