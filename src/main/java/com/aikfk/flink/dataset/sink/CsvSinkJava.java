package com.aikfk.flink.dataset.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/8 11:14 下午
 */
public class CsvSinkJava {
    public static void main(String[] args) throws Exception {
        // 准备环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataSet<String> dateSource = env.fromElements(
                "java java spark hive",
                "hive java java spark",
                "java java hadoop"
        );

        /**
         * String -> flatMap() -> groupBy() -> reduceGroup() -> Tuple2
         */
        DataSet<Tuple2<String,Integer>> wordcount = dateSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : s.split(" ")){
                    collector.collect(new Tuple2<>(word,1));
                }
            }
        }).groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void reduce(Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String key = null;
                int count = 0;
                for (Tuple2<String, Integer> tuple2 : iterable){
                    key = tuple2.f0;
                    count = count + tuple2.f1;
                }
                collector.collect(new Tuple2<>(key,count));
            }
        });

        // 写入到csv文件里，指定行切割符、列切割符
        wordcount.writeAsCsv("/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/csvfile.csv", "\n",",");

        env.execute();
    }
}
