package com.aikfk.flink.datastream.processfunction;

import com.aikfk.flink.datastream.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/23 3:04 下午
 */
public class ProcessOnTimer {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("bigdata-pro-m07", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        //3.使用ProcessFunction的定时器功能
        SingleOutputStreamOperator<WaterSensor> result = waterSensorDS.keyBy(WaterSensor::getId).process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context context, Collector<WaterSensor> collector) throws Exception {
                //获取当前数据的处理时间
                long ts = context.timerService().currentProcessingTime();
                System.out.println(ts);

                //注册定时器，当前数据的处理时间 + 5秒
                context.timerService().registerProcessingTimeTimer(ts + 5000L);

                //输出数据
                collector.collect(value);
            }

           //注册的定时器响起,触发动作
           @Override
           public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
               System.out.println("定时器触发:" + timestamp);
           }
        });

        //4.打印数据
        result.print();

        //5.执行任务
        env.execute();
    }
}
