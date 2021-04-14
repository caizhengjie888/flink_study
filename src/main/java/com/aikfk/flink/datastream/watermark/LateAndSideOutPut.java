package com.aikfk.flink.datastream.watermark;

import com.aikfk.flink.datastream.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author ：caizhengjie
 * @description：基于事件事件滚动窗口测试watermark机制
 * @date ：2021/3/20 9:21 下午
 */
public class LateAndSideOutPut {

    public static void main(String[] args) throws Exception {

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("bigdata-pro-m07", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
                    }
                });

        // 3.提取数据中的时间戳字段
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorDS
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        // 设置最大允许的延迟时间
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        // 指定事时间件列
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));

        // 4.按照id分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorSingleOutputStreamOperator.keyBy(WaterSensor::getId);

        // 5.开窗，允许迟到数据，侧输出流
        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<WaterSensor>("Side") {
                });

        // 6.计算总和
        SingleOutputStreamOperator<WaterSensor> result = window.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor t1, WaterSensor t2) throws Exception {
                return new WaterSensor(t1.getId(),t1.getTs(),t1.getVc() + t2.getVc());
            }
        });
        DataStream<WaterSensor> sideOutput = result.getSideOutput(new OutputTag<WaterSensor>("Side") {
        });

        // 7.打印
        result.print();
        sideOutput.print("Side");

        // 8.执行任务
        env.execute();
    }
}
