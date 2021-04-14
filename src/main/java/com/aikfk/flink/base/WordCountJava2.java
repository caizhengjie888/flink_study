package com.aikfk.flink.base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/5 3:07 下午
 */
public class WordCountJava2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String,Integer>> dataStream = env.socketTextStream("bigdata-pro-m07",9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] words = s.split(" ");
                        for (String word : words){
                            collector.collect(new Tuple2<>(word,1));
                        }
                    }
                })
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);

        dataStream.print();

        env.execute("Window WordCount");
    }

//    public static class Splitter implements FlatMapFunction<String,Tuple2<String,Integer>> {
//
//        @Override
//        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
//
//            for (String word : sentence.split(" ")){
//                out.collect(new Tuple2<String,Integer>(word,1));
//            }
//        }
//    }
}
