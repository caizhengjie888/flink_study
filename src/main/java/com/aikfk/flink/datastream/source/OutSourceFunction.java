package com.aikfk.flink.datastream.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/10 9:39 下午
 */
public class OutSourceFunction implements SourceFunction<Tuple2<Integer,Integer>> {

    private int count = 0;
    @Override
    public void run(SourceContext sourceContext) throws Exception {

        while (count < 1000){
            int first = (int) (Math.random() * 10);
            sourceContext.collect(new Tuple2<>(first,first));
            count++;
            Thread.sleep(100L);
        }

    }

    @Override
    public void cancel() {

    }
}
