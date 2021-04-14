package com.aikfk.flink.datastream.window;

import com.aikfk.flink.base.MySource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/25 1:41 下午
 */
public class ContinueProcessWindowFunction {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env
                .addSource(new MySource())
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] words = s.split(",");
                        return new Tuple2<>(words[0] , Long.parseLong(words[1]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1000L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> t1, long l) {
                                return t1.f1;
                            }
                        }))
                .keyBy(key -> key.f0)
                .timeWindow(Time.seconds(1000L))
                .aggregate(new AggregateFunction<Tuple2<String, Long>, Tuple2<String,Integer>, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String,Integer> createAccumulator() {
                        return new Tuple2<>("" , 0);
                    }

                    @Override
                    public Tuple2<String,Integer> add(Tuple2<String, Long> t1,
                                                      Tuple2<String,Integer> t2) {
                        return new Tuple2<>(t1.f0 , t2.f1 + 1);
                    }

                    @Override
                    public Tuple2<String,Integer> getResult(Tuple2<String,Integer> addValue) {
                        return addValue;
                    }

                    @Override
                    public Tuple2<String,Integer> merge(Tuple2<String,Integer> value1, Tuple2<String,Integer> acc1) {
                        return new Tuple2<>(value1.f0 , value1.f1 + acc1.f1);
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(2L)))
                .process(new ProcessAllWindowFunction<Tuple2<String, Integer>, Object, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Object> collector) throws Exception {
                        for (Tuple2<String,Integer> t1 : iterable) {
                            System.out.println(t1);
                        }
                    }
                })
                .print();


//        dataStream.print();

        env.execute("Window WordCount");
    }
}
