package com.aikfk.flink.datastream.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/10 9:38 下午
 */
public class OutSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Integer,Integer>> stream = env.addSource(new OutSourceFunction());

        stream.print();
        env.execute("stream");
    }
}
