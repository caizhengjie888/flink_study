package com.aikfk.flink.datastream.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/19 10:10 上午
 */
public class TimeWindowFunction {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("bigdata-pro-m07",9999);

        //3.压平并转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });

        //4.按照单词分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDS.keyBy(data -> data.f0);

        //5.开窗
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        /**
         * 6.增量聚合窗口计算方式一：reduce
         */
        DataStream<Tuple2<String,Integer>> result1 = windowedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return new Tuple2<>(t1.f0,t1.f1 + t2.f1);
            }
        });

        /**
         * 6.增量聚合窗口计算方式二：map
         */
        DataStream<Tuple2<String,Integer>> result2 = windowedStream.sum(1);

        /**
         * 6.增量聚合窗口计算方式三：aggregate
         * 既能进行增量聚合，又能拿到窗口信息
         */
        DataStream<Tuple2<String, Integer>> result3 = windowedStream.aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
            // 创建累加器: 初始化中间值
            @Override
            public Integer createAccumulator() {
                return 0;
            }

            // 累加器操作
            @Override
            public Integer add(Tuple2<String, Integer> stringIntegerTuple2, Integer integer) {
                return integer + 1;
            }

            // 获取结果
            @Override
            public Integer getResult(Integer integer) {
                return integer;
            }

            // 累加器的合并: 只有会话窗口才会调用
            @Override
            public Integer merge(Integer a, Integer b) {
                return a + b;
            }
        }, new WindowFunction<Integer, Tuple2<String, Integer>, String, TimeWindow>() {
            @Override
            public void apply(String key, TimeWindow timeWindow, Iterable<Integer> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 取出迭代器中的数据
                Integer next = iterable.iterator().next();

                // 输出数据并计算出窗口时间(既能进行增量聚合，又能拿到窗口信息)
                collector.collect(new Tuple2<>(new Timestamp(timeWindow.getStart()) + ":" + key,next));

            }
        });


        /**
         * 6.全量聚合窗口计算方式一：apply
         * 进行全量聚合，拿到窗口信息
         */
        DataStream<Tuple2<String,Integer>> result4 = windowedStream.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
            @Override
            public void apply(String key, TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 取出迭代器的长度
                ArrayList<Tuple2<String, Integer>> arrayList = Lists.newArrayList(iterable.iterator());
                // 输出数据并计算出窗口时间（进行全量聚合，拿到窗口信息）
                collector.collect(new Tuple2<>(new Timestamp(timeWindow.getStart()) + ":" + key,arrayList.size()));

            }
        });

        /**
         * 6.全量聚合窗口计算方式二：process
         * 进行全量聚合，拿到窗口信息
         */
        DataStream<Tuple2<String,Integer>> result5 = windowedStream.process(
                new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
            @Override
            public void process(String key, Context context, Iterable<Tuple2<String, Integer>> iterable,
                                Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 取出迭代器的长度
                ArrayList<Tuple2<String, Integer>> arrayList = Lists.newArrayList(iterable.iterator());
                // 输出数据并计算出窗口时间（进行全量聚合，拿到窗口信息）
                collector.collect(new Tuple2<>(new Timestamp(context.window().getStart()) + ":" + key,arrayList.size()));
            }
        });

        //7.打印
        result5.print();

        //8.执行任务
        env.execute();
    }
}
