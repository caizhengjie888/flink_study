package com.aikfk.flink.dataset.iteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/9 5:59 下午
 */
public class PiIterator {
    public static void main(String[] args) throws Exception {
        // 准备环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 创建初始IterativeDataSet
        IterativeDataSet<Integer> initial = env.fromElements(0).iterate(10000);

        DataSet<Integer> iteration = initial.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer integer) throws Exception {
                double x = Math.random();
                double y = Math.random();
                return integer + ((x * x + y * y <= 1 ) ? 1 : 0);
            }
        });

        // 计算出点的距离小于一的个数
        DataSet<Integer> count = initial.closeWith(iteration);

        // 求出PI
        DataSet<Double> result = count.map(new MapFunction<Integer, Double>() {
            @Override
            public Double map(Integer count) throws Exception {
                return count / (double) 10000 * 4;
            }
        });

        result.print();

        /**
         * 3.146
         */
    }
}
