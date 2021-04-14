package com.aikfk.flink.dataset.source;

import com.aikfk.flink.base.WordCountPOJO;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/7 2:04 下午
 */
public class CsvSourceJava {
    public static void main(String[] args) throws Exception {

        // 准备环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取csv数据（方式一：映射POJO类对象）
        DataSet<WordCountPOJO> dataSet1 = env.readCsvFile("/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/wordcount.csv")
                .pojoType(WordCountPOJO.class,"word","count")
                .groupBy("word")
                .reduce(new ReduceFunction<WordCountPOJO>() {
                    @Override
                    public WordCountPOJO reduce(WordCountPOJO t1, WordCountPOJO t2) throws Exception {
                        return new WordCountPOJO(t1.word, t1.count + t2.count);
                    }
                });

        dataSet1.print();

        /**
         * WordCount{word='java', count=22}
         * WordCount{word='spark', count=5}
         * WordCount{word='hive', count=7}
         */

        // 读取csv数据（方式二：映射成Tuple类，带有三个字段）
        DataSet<Tuple3<String,Integer,Integer>> dataSet2 = env.readCsvFile("/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/wordcount.csv")
                .types(String.class,Integer.class,Integer.class)
                .groupBy(0)
                .reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> t1, Tuple3<String, Integer, Integer> t2) throws Exception {
                        return new Tuple3<>(t1.f0,t1.f1 + t2.f1,t2.f2);
                    }
                });

        dataSet2.print();

        /**
         * (java,22,1)
         * (spark,5,2)
         * (hive,7,1)
         */


        // 读取csv数据（方式二：映射成Tuple类，带有三个字段，只取两个字段）
        DataSet<Tuple2<String,Integer>> dataSet3 = env.readCsvFile("/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/wordcount.csv")
                // 只取了第一个第二个字段（1表示取，2表示没有取）
                .includeFields("110")
                .types(String.class,Integer.class)
                .groupBy(0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
                    }
                });

        dataSet3.print();

        /**
         * (java,22)
         * (spark,5)
         * (hive,7)
         */

    }
}
