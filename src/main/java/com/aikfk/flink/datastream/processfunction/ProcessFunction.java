package com.aikfk.flink.datastream.processfunction;

import com.aikfk.flink.base.MySource;
import com.aikfk.flink.base.Tools;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/26 1:32 下午
 */
public class ProcessFunction {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.生成dataStream1，window join之前必须要生成WM，即实现assignTimestampsAndWatermarks方法
        DataStream<Tuple2<String,Long>> dataStream = env.addSource(new MySource()).map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] words = s.split(",");
                return new Tuple2<>(words[0] , Long.parseLong(words[1]));
            }
        })
        // 3.生成watermark
        .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ofMinutes(1L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String,Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String,Long> input, long l) {
                                return input.f1;
                            }
                        }))


        // 4.keyby
        .keyBy(key -> key.f0)
        // 实现processfunction方法
        .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>() {

            private ValueState<CountWithTimestamp> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(
                        new ValueStateDescriptor<CountWithTimestamp>("mystate",CountWithTimestamp.class));
            }

            @Override
            public void processElement(Tuple2<String, Long> value,
                                       Context context, Collector<Tuple2<String, Long>> collector) throws Exception {

                CountWithTimestamp currentElement = state.value();
                if (currentElement == null){
                    currentElement = new CountWithTimestamp();
                    currentElement.key = value.f0;
                }

                // 对key进行累加
                currentElement.count ++;
                currentElement.lastModified = context.timestamp();
                state.update(currentElement);

                // 注册定时器
                context.timerService().registerEventTimeTimer(currentElement.lastModified + 1000);

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                CountWithTimestamp result = state.value();

                System.out.println(ctx.getCurrentKey()+"   timestamp : "+ Tools.getMsToDate(timestamp) +
                        " ctx.timestamp :"+ Tools.getMsToDate(ctx.timestamp())+
                        " lastModified:"+Tools.getMsToDate(result.lastModified));

                if ((result.lastModified + 1000) == timestamp){
                    out.collect(new Tuple2<>(result.key, result.count));
                }
            }
        });

        dataStream.print();

        env.execute("Window WordCount");
    }

    private static class CountWithTimestamp{
        private String key;
        private long count;
        private long lastModified;

    }
}
