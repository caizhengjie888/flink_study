package com.aikfk.flink.dataset.transform;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/7 8:48 下午
 */
public class MapPartitionJava {
    public static void main(String[] args) throws Exception {
        // 准备环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> dateSource = env.fromElements(
                "java java spark hive",
                "hive java java spark",
                "java java hadoop"
        );

        /**
         * mapPartition
         */
        DataSet<String> mapPartition = dateSource.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> iterable, Collector<String> collector) throws Exception {
                for (String line : iterable){
                    collector.collect(line.toUpperCase());
                }
            }
        });

        mapPartition.print();

        /**
         * JAVA JAVA SPARK HIVE
         * HIVE JAVA JAVA SPARK
         * JAVA JAVA HADOOP
         */
    }
}
