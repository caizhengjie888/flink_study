package com.aikfk.flink.dataset.semantic;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/9 9:22 下午
 */
public class ForwardedFields {
    public static void main(String[] args) throws Exception {

        // 准备环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<String,String,Integer>> csvSource = env
                .readCsvFile("/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/employee.csv")
                .types(String.class,String.class,Integer.class);

        csvSource.map(new MyMapFunction()).print();
    }

    @FunctionAnnotation.ForwardedFields("f0 -> f3")
    public static class MyMapFunction implements MapFunction<Tuple3<String,String,Integer>, Tuple4<String,String,Integer,String>>{
        @Override
        public Tuple4<String, String, Integer, String> map(
                Tuple3<String, String, Integer> tuple3) throws Exception {
            return new Tuple4<>(tuple3.f0, tuple3.f1, tuple3.f2,tuple3.f0);
        }
    }
}
