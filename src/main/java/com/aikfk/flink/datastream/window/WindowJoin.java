package com.aikfk.flink.datastream.window;

import com.aikfk.flink.base.MySource;
import com.aikfk.flink.base.MySource2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.time.Duration;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/25 1:57 下午
 */
public class WindowJoin {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.生成dataStream1，window join之前必须要生成WM，即实现assignTimestampsAndWatermarks方法
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

        // 3.生成dataStream2，window join之前必须要生成WM，即实现assignTimestampsAndWatermarks方法
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
        
        // TumblingEvent window join
        dataStream1.join(dataStream2)
                .where(key -> key.f0)
                .equalTo(key -> key.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1L)))
                .apply(new JoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, Object>() {
                    @Override
                    public Object join(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
                        return new Tuple4<>(t1.f0,t1.f1,t2.f0,t2.f1);
                    }
                })
                .print();

        env.execute("Window WordCount");
    }
}
