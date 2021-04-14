package com.aikfk.flink.datastream.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/12 7:38 下午
 */
public class PartitionCustom {
    public static void main(String[] args) throws Exception {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String,Integer>> dataStream = env.socketTextStream("bigdata-pro-m07",9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        for (String word : s.split(" ")){
                            collector.collect(new Tuple2<>(word,1));
                        }
                    }
                });

        /**
         * 自定义分区
         */
        dataStream.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {

                int partition = key.hashCode() % numPartitions;

                System.out.println("key: " + key + " partition: " + partition + " numPartitions: " + numPartitions);

                return partition;
            }
        }, new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });

        env.execute("partition");
        
    }
}
