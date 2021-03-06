package com.aikfk.flink.datastream.state;

import com.aikfk.flink.datastream.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/4/1 2:09 下午
 */
public class KeyedStateTest {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("bigdata-pro-m07", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
                    }
                });

        // 3.按照id分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDS.keyBy(WaterSensor::getId);

        // 4.演示状态的使用
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.process(new MyStateProcessFunc());

        // 5.打印
        result.print();

        // 6.执行任务
        env.execute();

    }

    public static class MyStateProcessFunc extends KeyedProcessFunction<String,WaterSensor,WaterSensor> {


        // a.定义状态
        private ValueState<Long> valueState;
        private ListState<Long> listState;
        private MapState<String,Long> mapState;
        private ReducingState<WaterSensor> reducingState;
        private AggregatingState<WaterSensor,WaterSensor> aggregatingState;


        // b.初始化
        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<Long>("value-state",Long.class));


            listState = getRuntimeContext()
                    .getListState(new ListStateDescriptor<Long>("list-state", Long.class));

            mapState = getRuntimeContext()
                    .getMapState(new MapStateDescriptor<String, Long>("map-state", String.class,Long.class));

//            reducingState = getRuntimeContext()
//                    .getReducingState(new ReducingStateDescriptor<WaterSensor>())

//            aggregatingState = getRuntimeContext()
//                    .getAggregatingState(new AggregatingStateDescriptor
//                            <WaterSensor, Object, WaterSensor>());

        }


        // c.状态的使用
        @Override
        public void processElement(WaterSensor value,
                                   Context context, Collector<WaterSensor> collector) throws Exception {

            // c.1 Value状态
            Long value1 = valueState.value();
            valueState.update(122L);
            valueState.clear();

            // c.2 List状态
            Iterable<Long> longs = listState.get();
            listState.add(122L);
            listState.clear();
            listState.update(new ArrayList<>());

            // c.3 Map状态
            Iterator<Map.Entry<String,Long>> iterator = mapState.iterator();
            Long aLong = mapState.get("");
            mapState.contains("");
            mapState.put("",122L);
            mapState.putAll(new HashMap<>());
            mapState.remove("");
            mapState.clear();

            //c.4 Reduce状态
            WaterSensor waterSensor1 = reducingState.get();
            reducingState.add(new WaterSensor());
            reducingState.clear();

            //c.5 Agg状态
            aggregatingState.add(value);
            WaterSensor waterSensor2 = aggregatingState.get();
            aggregatingState.clear();

        }
    }
}
