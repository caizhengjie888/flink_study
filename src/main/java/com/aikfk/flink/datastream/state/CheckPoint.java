package com.aikfk.flink.datastream.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/31 4:04 下午
 */
public class CheckPoint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // start a checkpoint every 1000ms
        env.enableCheckpointing(1000);
        // advanced options
        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 如果有更近的保存点时，是否将作业回退到该检查点
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.setStateBackend(new FsStateBackend(
                "file:////Users/caizhengjie/Desktop/test ",true));

        DataStreamSink<Tuple2<String,Long>> dataStream = env.addSource(new MySource())
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String line) throws Exception {
                        String[] words = line.split(",");
                        return new Tuple2<>(words[0],Long.parseLong(words[1]));
                    }
                })
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


    public static class MySource implements SourceFunction<String> {
        @Override
        public void cancel() {

        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            String data = "s,4";
            while (true) {
                ctx.collect(data);
            }
        }
    }
}
