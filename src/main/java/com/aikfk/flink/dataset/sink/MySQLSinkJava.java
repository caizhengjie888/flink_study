package com.aikfk.flink.dataset.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcOutputFormat;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/8 11:14 下午
 */
public class MySQLSinkJava {
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

        /**
         * Tuple2 -> map() -> Row
         */
        DataSet<Row> wordcountMysql = wordcount.map(new MapFunction<Tuple2<String, Integer>, Row>() {
            @Override
            public Row map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return Row.of(stringIntegerTuple2.f0,stringIntegerTuple2.f1);
            }
        });

        // write Tuple DataSet to a relational database
        wordcountMysql.output(
                // build and configure OutputFormat
                JdbcOutputFormat.buildJdbcOutputFormat()
                        .setDrivername("com.mysql.jdbc.Driver")
                        .setDBUrl("jdbc:mysql://bigdata-pro-m07:3306/flink?serverTimezone=GMT%2B8&useSSL=false")
                        .setUsername("root")
                        .setPassword("199911")
                        .setQuery("insert into wordcount (word, count) values (?,?)")
                        .finish()
        );

        env.execute();
    }
}
