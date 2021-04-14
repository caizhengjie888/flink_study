package com.aikfk.flink.datastream.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/19 10:10 上午
 */
public class TimeSession {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("bigdata-pro-m07",9999);

        //3.压平并转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });

        //4.按照单词分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDS.keyBy(data -> data.f0);

        //5.开启会话窗口，会话间隔时间为5s
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5 )));

        /**
         * 6.增量聚合窗口计算方式一：reduce
         */
        DataStream<Tuple2<String,Integer>> result = windowedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return new Tuple2<>(t1.f0,t1.f1 + t2.f1);
            }
        });

        //7.打印
        result.print();

        //8.执行任务
        env.execute();
    }
}
