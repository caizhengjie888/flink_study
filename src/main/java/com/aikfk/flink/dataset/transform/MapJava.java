package com.aikfk.flink.dataset.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/8 9:58 上午
 */
public class MapJava {
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

        mapSource.print();

        /**
         * JAVA JAVA SPARK HIVE
         * HIVE JAVA JAVA SPARK
         * JAVA JAVA HADOOP
         */
    }
}
