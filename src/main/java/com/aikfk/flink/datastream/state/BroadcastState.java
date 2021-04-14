package com.aikfk.flink.datastream.state;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/31 4:04 下午
 */
public class BroadcastState {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.读取流中的数据
        DataStream<Tuple2<String,Long>> eventDataStream = env.fromElements(
                Tuple2.of("a", 3L),
                Tuple2.of("a", 5L),
                Tuple2.of("b", 7L),
                Tuple2.of("c", 4L),
                Tuple2.of("c", 2L))
                .keyBy(value -> value.f0);

        DataStream<Long> dataStream = env.fromElements(3L);

        // 3.一个 map descriptor，它描述了用于存储规则名称与规则本身的 map 存储结构
        MapStateDescriptor<String,Long> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Long>() {}));

        // 4.广播流，广播规则并且创建 broadcast state
        BroadcastStream<Long> broadcastStream = dataStream
                .broadcast(ruleStateDescriptor);

        // 5.连接数据和广播流
        BroadcastConnectedStream<Tuple2<String, Long>, Long> connectedStream = eventDataStream.connect(broadcastStream);

        // 6.处理连接之后的流
        connectedStream.process(new RuleKeyedBroadcastProcessFunction()).print();

        //6.执行任务
        env.execute("broadcastStream");
    }

    /**
     * KeyedBroadcastProcessFunction 中的类型参数表示：
     * 1. key stream 中的 key 类型
     * 2. 非广播流中的元素类型
     * 3. 广播流中的元素类型
     * 4. 结果的类型，在这里是 string
     */
    static class RuleKeyedBroadcastProcessFunction extends
            KeyedBroadcastProcessFunction<String,Tuple2<String, Long>,Long,Object>{

        @Override
        public void open(Configuration parameters) throws Exception {
            //连接数据库，去配置参数的信息
        }

        // 一个 map descriptor，它描述了用于存储规则名称与规则本身的 map 存储结构
        MapStateDescriptor<String,Long> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Long>() {}));


        @Override
        public void processElement(Tuple2<String, Long> inputEventData,
                                   ReadOnlyContext readOnlyContext, Collector<Object> collector) throws Exception {

            // 获取广播状态
            ReadOnlyBroadcastState<String,Long> readOnlyBroadcastState = readOnlyContext.getBroadcastState(ruleStateDescriptor);
            if (readOnlyBroadcastState.contains("rule")){
                if (inputEventData.f1 > readOnlyBroadcastState.get("rule")){
                    collector.collect(inputEventData.f0  + " : " + inputEventData.f1);
                }
            }
        }

        @Override
        public void processBroadcastElement(Long broadcastValue, Context context,
                                            Collector<Object> collector) throws Exception {

            //config_name  ,  config_value
            context.getBroadcastState(ruleStateDescriptor).put("rule" ,broadcastValue);

        }
    }
}
