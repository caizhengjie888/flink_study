package com.aikfk.flink.datastream.transform;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/11 1:22 下午
 */
public class ConnectJava {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String,Integer>> dataStreamSource1 = env.fromElements(new Tuple2<>("spark",1),new Tuple2<>("java",3));
        DataStream<Tuple2<String,Integer>> dataStreamSource2 = env.fromElements(new Tuple2<>("hive",2),new Tuple2<>("hadoop",5));

        /**
         * connect()
         */
        ConnectedStreams<Tuple2<String, Integer>, Tuple2<String, Integer>> connectedStreams = dataStreamSource1
                .connect(dataStreamSource2).keyBy(0,0);

        DataStream<Tuple2<String,Integer>> mapResult = connectedStreams.map(new CoMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map1(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + 10);
            }

            @Override
            public Tuple2<String, Integer> map2(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2;
            }
        });

        mapResult.print();


        env.execute("stream");
    }
}
