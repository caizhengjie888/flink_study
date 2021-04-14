package com.aikfk.flink.datastream.window;

import com.aikfk.flink.base.MySource;
import com.aikfk.flink.base.MySource2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/25 1:57 下午
 */
public class IntervalJoin {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.生成dataStream1，interval join之前必须要生成WM，即实现assignTimestampsAndWatermarks方法
        DataStream<Tuple2<String,Long>> dataStream1 = env.addSource(new MySource()).map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] words = s.split(",");
                return new Tuple2<>(words[0] , Long.parseLong(words[1]));
            }
        })
        .assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ofMinutes(1L))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String,Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String,Long> input, long l) {
                        return input.f1;
                    }
                }));

        // 3.生成dataStream2，interval join之前必须要生成WM，即实现assignTimestampsAndWatermarks方法
        DataStream<Tuple2<String,Long>> dataStream2 = env.addSource(new MySource2()).map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] words = s.split(",");
                return new Tuple2<>(words[0] , Long.parseLong(words[1]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ofMinutes(1L))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String,Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String,Long> input, long l) {
                        return input.f1;
                    }
                }));
        
        // interval join
        dataStream1.keyBy(key -> key.f0)
                .intervalJoin(dataStream2.keyBy(key -> key.f0))
                .between(Time.seconds(0L),Time.seconds(2L))
                .process(new ProcessJoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, Object>() {
                    @Override
                    public void processElement(Tuple2<String, Long> t1,
                                               Tuple2<String, Long> t2,
                                               Context context, Collector<Object> collector) throws Exception {
                        collector.collect(new Tuple4<>(t1.f0,t1.f1,t2.f0,t2.f1));
                    }
                }).print();

        env.execute("Window WordCount");
    }
}
