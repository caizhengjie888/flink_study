package com.aikfk.flink.datastream.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/31 4:04 下午
 */
public class KeyedStateCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String,Long>> dataStream = env.fromElements(
                Tuple2.of("a", 3L),
                Tuple2.of("a", 5L),
                Tuple2.of("b", 7L),
                Tuple2.of("c", 4L),
                Tuple2.of("c", 2L))
                .keyBy(value -> value.f0)
                .flatMap(new CountFunction());

        dataStream.print();
        env.execute("KeyedState");
    }

    static class CountFunction extends RichFlatMapFunction<Tuple2<String,Long>,Tuple2<String,Long>>{

        // 定义状态ValueState
        private ValueState<Tuple2<String,Long>> keyCount;

        /**
         * 初始化
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple2<String,Long>> descriptor =
                    new ValueStateDescriptor<Tuple2<String, Long>>("keycount",
                            TypeInformation.of(new TypeHint<Tuple2<String,Long>>() {}));

            keyCount = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<String, Long> input,
                            Collector<Tuple2<String, Long>> collector) throws Exception {

            // 使用状态
            Tuple2<String, Long> currentValue =
                    (keyCount.value() == null) ? new Tuple2<>("", 0L) : keyCount.value();

            // 累加数据
            currentValue.f0 = input.f0;
            currentValue.f1 ++;

            // 更新状态
            keyCount.update(currentValue);
            collector.collect(keyCount.value());
        }
    }
}
