package com.aikfk.flink.datastream.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/31 4:04 下午
 */
public class QueryState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String,Long>> dataStream = env.addSource(new MySource())
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String line) throws Exception {
                        String[] words = line.split(",");
                        return new Tuple2<>(words[0],Long.parseLong(words[1]));
                    }
                })
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


            descriptor.setQueryable("query-name");
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

    public static class MySource implements SourceFunction<String> {
        @Override
        public void cancel() {

        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            String data = "a,4";
            while (true) {
                Thread.sleep(1000);
                ctx.collect(data);
            }
        }
    }
}
