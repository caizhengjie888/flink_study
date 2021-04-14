package com.aikfk.flink.datastream.project;

import com.aikfk.flink.datastream.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/17 2:03 下午
 */
public class FlinkPVProcess {

    public static void main(String[] args) throws Exception {

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.读取数据
        DataStreamSource<String> dataStreamSource =  env.readTextFile("/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/UserBehavior.csv");

        // 3.转换为JavaBean并过滤出PV数据
        SingleOutputStreamOperator<UserBehavior> fliterResult = dataStreamSource.flatMap(new FlatMapFunction<String, UserBehavior>() {
            @Override
            public void flatMap(String line, Collector<UserBehavior> collector) throws Exception {
                String[] word = line.split(",");

                collector.collect(new UserBehavior(
                        Long.parseLong(word[0]),
                        Long.parseLong(word[1]),
                        Integer.parseInt(word[2]),
                        word[3],
                        Long.parseLong(word[4])
                ));
            }
        }).filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                String PV = "pv";
                return PV.equals(userBehavior.getBehavior());
            }
        });

        // 4.指定key分组
        KeyedStream<UserBehavior, String> keyedStream = fliterResult.keyBy(UserBehavior::getBehavior);

        // 5.计算总和
        SingleOutputStreamOperator<Integer> result = keyedStream.process(new KeyedProcessFunction<String, UserBehavior, Integer>() {
            int count = 0;
            @Override
            public void processElement(UserBehavior userBehavior, Context context, Collector<Integer> collector) throws Exception {
                count++;
                collector.collect(count);

            }
        });

        // 6.打印输出
        result.print();

        // 7.执行任务
        env.execute();
    }
}
