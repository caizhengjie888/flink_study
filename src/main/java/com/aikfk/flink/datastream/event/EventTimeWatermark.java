package com.aikfk.flink.datastream.event;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author ：caizhengjie
 * @description：TODO
 * key,time
 * @date ：2021/3/13 11:03 上午
 */
public class EventTimeWatermark {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStreamSource = env.socketTextStream("bigdata-pro-m07", 9999);

        DataStream<Tuple3<String, Long, Integer>> dataStreamWatermark = dataStreamSource.map(new MapFunction<String, Tuple3<String, Long, Integer>>() {

            @Override
            public Tuple3<String, Long, Integer> map(String line) throws Exception {
                String[] word = line.split(",");
                return new Tuple3<>(word[0], Long.parseLong(word[1]), 1);
            }
            // 设置watermark = 当前最大事件时间 - 最大允许的延迟时间或乱序时间
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                // 设置最大允许的延迟时间
                .<Tuple3<String, Long, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                // 指定事时间件列
                .withTimestampAssigner((event, timestamp) -> event.f1))
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(2);


        dataStreamWatermark.print();
        env.execute("Watermark");
    }
}
