package com.aikfk.flink.datastream.source;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/10 9:29 下午
 */
public class FileSource {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream = env.readFile(
                new TextInputFormat(new Path("/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/employee.csv"))
                , "/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/employee.csv");

        stream.print();
        env.execute("stream");
    }
}
