package com.aikfk.flink.datastream.project;

import com.aikfk.flink.datastream.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple2;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/17 2:03 下午
 */
public class FlinkPV {

    public static void main(String[] args) throws Exception {

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.读取数据
        DataStreamSource<String> dataStreamSource =  env.readTextFile("/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/UserBehavior.csv");

        // 3.转换为JavaBean并过滤出PV数据
        SingleOutputStreamOperator<Tuple2<String,Integer>> fliterResult = dataStreamSource.flatMap(new FlatMapFunction<String, UserBehavior>() {
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
        }).map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior userBehavior) throws Exception {
                return new Tuple2<String,Integer>(userBehavior.getBehavior(),1);
            }
        });

        // 4.指定key分组
        KeyedStream<Tuple2<String, Integer>, Object> keyedStream = fliterResult.keyBy(value -> value._1);

        // 5.计算总和
        DataStream<Tuple2<String,Integer>>  result = keyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return new Tuple2<>(t1._1,t1._2 + t2._2);
            }
        });

        // 6.打印输出
        result.print();

        // 7.执行任务
        env.execute();
    }
}
