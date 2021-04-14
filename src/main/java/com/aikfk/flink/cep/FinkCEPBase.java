package com.aikfk.flink.cep;

import com.google.inject.internal.asm.$ByteVector;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.apache.flink.api.common.eventtime.WatermarkStrategy.forBoundedOutOfOrderness;

public class FinkCEPBase {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env
                = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<Tuple3<String, Long, String>> dataStreamSource = env.fromElements(
                Tuple3.of("user_1", 1609399520L, "buy"),
                Tuple3.of("user_2", 1609399410L, "login"),
                Tuple3.of("user_2", 1609399420L, "login"),
                Tuple3.of("user_2", 1609399425L, "login"),
                Tuple3.of("user_3", 1609399435L, "login"),
                Tuple3.of("user_3", 1609399455L, "buy")
        );

        KeyedStream<Tuple3<String, Long, String>, String> keyedStream
                = dataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple3<String, Long, String>>forBoundedOutOfOrderness(Duration.ofMinutes(1L))
                .withTimestampAssigner((event, input) -> event.f1))
                .keyBy(key -> key.f0);

        Pattern<Tuple3<String, Long, String>, Tuple3<String, Long, String>>
                pattern = Pattern.<Tuple3<String, Long, String>>begin("start")
                .where(new IterativeCondition<Tuple3<String, Long, String>>() {

                    @Override
                    public boolean filter(Tuple3<String, Long, String>
                                                  stringLongStringTuple3,
                                          Context<Tuple3<String, Long, String>> context) throws Exception {
                        return stringLongStringTuple3.f2.equals("login");
                    }
                }).within(Time.milliseconds(10)).times(3);

        PatternStream<Tuple3<String, Long, String>> pattern1 = CEP.pattern(keyedStream, pattern);

        pattern1.process(new PatternProcessFunction<Tuple3<String, Long, String>, Object>() {
            @Override
            public void processMatch(Map<String, List<Tuple3<String, Long, String>>> map,
                                     Context context, Collector<Object> collector) throws Exception {
                collector.collect(map);
            }
        }).print();

        env.execute();

    }

}
