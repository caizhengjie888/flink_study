package com.aikfk.flink.dataset.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/7 8:26 下午
 */
public class AggregateJava {
    public static void main(String[] args) throws Exception {
        // 准备环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> dateSource = env.fromElements(
                "java java spark hive",
                "hive java java spark",
                "java java hadoop"
        );

        /**
         * map
         */
        DataSet<String> mapSource = dateSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String line) throws Exception {
                return line.toUpperCase();
            }
        });

        /**
         * flatmap
         */
        DataSet<Tuple2<String,Integer>> flatmapSource = mapSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : s.split(" ")){
                    collector.collect(new Tuple2<>(word,1));
                }
            }
        });

        /**
         * aggregate
         */
        DataSet<Tuple2<String,Integer>> aggregateSource =  flatmapSource.groupBy(0).aggregate(Aggregations.SUM,1);

        aggregateSource.print();

        /**
         * (HIVE,2)
         * (HADOOP,1)
         * (JAVA,6)
         * (SPARK,2)
         */

    }
}
