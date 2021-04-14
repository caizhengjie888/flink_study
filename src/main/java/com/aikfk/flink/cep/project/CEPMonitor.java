package com.aikfk.flink.cep.project;

import com.aikfk.flink.cep.project.MonitorEventSource;
import com.aikfk.flink.cep.project.event.MonitorEvent;
import com.aikfk.flink.cep.project.event.TemperatureEvent;
import com.aikfk.flink.cep.project.event.TempratureAlert;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 如果服务器的温度超过一定的阈值的时候就应该告警，所以本次需求是对服务器
 * 的温度进行监控和报警
 * 需要：
 * 1.现在有一个服务器的阈值threld
 * 2.如果服务器温度2次超过这个阈值，我们就发出warn
 * 3.如果服务器在20秒之内发出两次warn，并且第二次告警的温度比第一次warn的告警温度高，就alert
 */
public class CEPMonitor {

    //温度的阈值
    private static final double temprature_thresold = 100 ;

    //生成机架ID使用的
    private static final int MaxRack_ID = 10;
    //多久生成一次数据 100MS
    private static final long  PAUSE = 100;
    //生成温度的概率
    private static final double TEMPERATURE_Ration = 0.5;
    //根据高斯分布来生成温度
    private static final double TEMPERATURE_STD = 20;
    private static final double TEMPERATURE_MEAN = 90;
    private static final double POWER_STD = 10;
    private static final double POWER_MEAN = 100;
    public static void main(String[] args)throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<MonitorEvent> monitorEvent = environment.addSource(new MonitorEventSource(
                MaxRack_ID,
                PAUSE,
                TEMPERATURE_Ration,
                TEMPERATURE_STD,
                TEMPERATURE_MEAN,
                POWER_STD,
                POWER_MEAN))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<MonitorEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event , input) -> event.getCurrentTime()))
                .keyBy(key -> key.getRackID());


        Pattern<MonitorEvent, TemperatureEvent> pattern_1 = Pattern.<MonitorEvent>begin("first")
                .subtype(TemperatureEvent.class)
                .where(new IterativeCondition<TemperatureEvent>() {
                    @Override
                    public boolean filter(TemperatureEvent temperatureEvent, Context<TemperatureEvent> context) throws Exception {
                        return temperatureEvent.getTemperature() >= temprature_thresold;
                    }
                })
                .next("second")
                .subtype(TemperatureEvent.class)
                .where(new IterativeCondition<TemperatureEvent>() {
                    @Override
                    public boolean filter(TemperatureEvent temperatureEvent, Context<TemperatureEvent> context) throws Exception {
                        return temperatureEvent.getTemperature() >= temprature_thresold;
                    }
                }).within(Time.seconds(10));

        PatternStream<MonitorEvent> patternStream_1= CEP.pattern(monitorEvent, pattern_1);

        KeyedStream<Tuple2<Integer, Double>, Integer> warnStream = patternStream_1.select(new PatternSelectFunction<MonitorEvent, Tuple2<Integer, Double>>() {
            @Override
            public Tuple2<Integer, Double> select(Map<String, List<MonitorEvent>> map) throws Exception {
                TemperatureEvent first = (TemperatureEvent) map.get("first").get(0);
                TemperatureEvent second = (TemperatureEvent) map.get("second").get(0);
                return new Tuple2<Integer, Double>(first.getRackID(), (first.getTemperature() + second.getTemperature()) / 2);
            }
        }).keyBy(key -> key.f0);


        Pattern<Tuple2<Integer, Double>, Tuple2<Integer, Double>> pattern_2
                = Pattern.<Tuple2<Integer, Double>>begin("first")
                .next("second")
                .within(Time.seconds(20));

        PatternStream<Tuple2<Integer, Double>> alertPatternStream = CEP.pattern(warnStream, pattern_2);

        DataStream<TempratureAlert> alertStream =
                alertPatternStream.flatSelect(new PatternFlatSelectFunction<Tuple2<Integer, Double>, TempratureAlert>() {
                    @Override
                    public void flatSelect(Map<String, List<Tuple2<Integer, Double>>> map,
                                           Collector<TempratureAlert> collector) throws Exception {
                        Tuple2<Integer, Double> first = map.get("first").get(0);
                        Tuple2<Integer, Double> second = map.get("second").get(0);

                        if (second.f1 > first.f1) {
                            collector.collect(new TempratureAlert(first.f0));
                        }
                    }
                });

        warnStream.print();
        alertStream.print();


        environment.execute();
    }

}
