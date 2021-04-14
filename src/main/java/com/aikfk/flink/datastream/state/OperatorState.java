package com.aikfk.flink.datastream.state;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/31 4:04 下午
 */
public class OperatorState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSink<Tuple2<String,Long>> dataStream = env.fromElements(
                Tuple2.of("a", 3L),
                Tuple2.of("a", 5L),
                Tuple2.of("b", 7L),
                Tuple2.of("c", 4L),
                Tuple2.of("c", 2L))
                .keyBy(value -> value.f0)
                .addSink(new BufferingSink());

        env.execute("KeyedState");
    }

    static class BufferingSink implements SinkFunction<Tuple2<String,Long>>, CheckpointedFunction {

        private ListState<Tuple2<String,Long>> listState;
        private List<Tuple2<String,Long>> bufferedElements = new ArrayList<>();


        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            ListStateDescriptor<Tuple2<String, Long>> descriptor =
                    new ListStateDescriptor<Tuple2<String, Long>>("bufferedSinkState",
                            TypeInformation.of(new TypeHint<Tuple2<String,Long>>() {}));

            listState = context.getOperatorStateStore().getListState(descriptor);

            if (context.isRestored()){
                for (Tuple2<String, Long> element : listState.get()){
                    bufferedElements.add(element);
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            for (Tuple2<String, Long> element : bufferedElements){
                listState.add(element);
            }
        }


        @Override
        public void invoke(Tuple2<String,Long> value, Context context) throws Exception {

            bufferedElements.add(value);
            System.out.println("invoke>>> " + value);
            for (Tuple2<String,Long> element : bufferedElements){
                System.out.println(Thread.currentThread().getId() + " >> " + element.f0 + " : " + element.f1);
            }
        }
    }
}
