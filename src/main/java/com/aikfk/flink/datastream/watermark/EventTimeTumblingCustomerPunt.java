package com.aikfk.flink.datastream.watermark;

import com.aikfk.flink.datastream.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author ：caizhengjie
 * @description：基于事件事件滚动窗口测试watermark机制
 * @date ：2021/3/20 9:21 下午
 */
public class EventTimeTumblingCustomerPunt {

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
                .assignTimestampsAndWatermarks(new WatermarkStrategy<WaterSensor>() {
                    @Override
                    public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new MyPunt(2000L);
                    }
                }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));

        // 4.按照id分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorSingleOutputStreamOperator.keyBy(WaterSensor::getId);

        // 5.开窗
        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        // 6.计算总和
        SingleOutputStreamOperator<WaterSensor> result = window.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor t1, WaterSensor t2) throws Exception {
                return new WaterSensor(t1.getId(),t1.getTs(),t1.getVc() + t2.getVc());
            }
        });

        // 7.打印
        result.print();

        // 8.执行任务
        env.execute();
    }

    /**
     * 自定义间歇性watermark
     * */
    public static class MyPunt implements WatermarkGenerator<WaterSensor> {

        private Long maxTs;
        private Long maxDelay;

        public MyPunt(Long maxDelay) {
            this.maxDelay = maxDelay;
            this.maxTs = Long.MIN_VALUE + maxDelay + 1;
        }

        //当数据来的时候调用
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("取数据中最大的时间戳");
            maxTs = Math.max(eventTimestamp, maxTs);
            output.emitWatermark(new Watermark(maxTs - maxDelay));
        }

        //周期性调用
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
        }
    }
}
