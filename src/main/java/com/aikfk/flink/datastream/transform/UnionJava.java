package com.aikfk.flink.datastream.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/11 1:22 下午
 */
public class UnionJava {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStreamSource1 = env.socketTextStream("bigdata-pro-m07",9999);

        DataStream<String> dataStreamSource2 = env.socketTextStream("bigdata-pro-m07",9998);

        DataStream<String> dataStreamSource = dataStreamSource1.union(dataStreamSource2);


        /**
         * flatmap()
         */
        DataStream<Tuple2<String, Integer>> flatmapResult = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : s.split(" ")){
                    collector.collect(new Tuple2<>(word,1));
                }
            }
        });

        /**
         * filter() -> keyBy() -> reduce()
         */
        DataStream<Tuple2<String,Integer>> result = flatmapResult.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                String saprk = "spark";
                return !stringIntegerTuple2.f0.equals(saprk);
            }
        })
            .keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        })
            .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                    return new Tuple2<>(t1.f0,t1.f1 + t2.f1);
                }
            });

        result.print();

        env.execute("stream");
    }
}
