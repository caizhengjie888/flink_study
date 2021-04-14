package com.aikfk.flink.datastream.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/10 9:29 下午
 */
public class SocketSource {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream = env.socketTextStream("bigdata-pro-m07",9999);

        stream.print();
        env.execute("stream");
    }
}
