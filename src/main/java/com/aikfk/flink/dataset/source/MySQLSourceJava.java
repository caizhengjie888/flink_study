package com.aikfk.flink.dataset.source;

import com.aikfk.flink.base.WordCountPOJO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.types.Row;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/7 5:08 下午
 */
public class MySQLSourceJava {
    public static void main(String[] args) throws Exception {
        // 准备环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取mysql数据源
        DataSet<Row> dbData = env.createInput(
                        JdbcInputFormat.buildJdbcInputFormat()
                                .setDrivername("com.mysql.jdbc.Driver")
                                .setDBUrl("jdbc:mysql://bigdata-pro-m07:3306/flink?serverTimezone=GMT%2B8&useSSL=false")
                                .setUsername("root")
                                .setPassword("199911")
                                .setQuery("select  * from wordcount")
                                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
                                .finish()
                );

        /**
         * DataSet中的数据类型是POJO类
         */
        DataSet<WordCountPOJO> dataSet = dbData.map(new MapFunction<Row, WordCountPOJO>() {
            @Override
            public WordCountPOJO map(Row row) throws Exception {
                return new WordCountPOJO(String.valueOf(row.getField(0)), (Integer) row.getField(1));
            }
        })
        .groupBy("word")
        .reduce(new ReduceFunction<WordCountPOJO>() {
            @Override
            public WordCountPOJO reduce(WordCountPOJO t1, WordCountPOJO t2) throws Exception {
                return new WordCountPOJO(t1.word,t1.count + t2.count);
            }
        });

        dataSet.print();

        /**
         * WordCount{word='java', count=5}
         * WordCount{word='spark', count=11}
         * WordCount{word='hive', count=5}
         */

//        /**
//         * DataSet中的数据类型是Tuple类
//         */
//        DataSet<Tuple2<String,Integer>> dataSet = dbData.map(new MapFunction<Row, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> map(Row row) throws Exception {
//                return new Tuple2<>(String.valueOf(row.getField(0)), (Integer) row.getField(1));
//            }
//        })
//        .groupBy(0)
//        .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
//                return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
//            }
//        });

    }
}
