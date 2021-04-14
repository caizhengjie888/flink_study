package com.aikfk.flink.datastream.transform;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/11 1:22 下午
 */
public class SideOut {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        OutputTag<Tuple2<String,Integer>> outputTag = new OutputTag<Tuple2<String,Integer>>("side-output"){};

        DataStream<Tuple2<String,Integer>> dataStreamSource = env.fromElements(
                new Tuple2<>("alex",11000),
                new Tuple2<>("lili",3200),
                new Tuple2<>("lucy",3400),
                new Tuple2<>("pony",13000),
                new Tuple2<>("tony",33000),
                new Tuple2<>("herry",4500),
                new Tuple2<>("cherry",9000),
                new Tuple2<>("jack",13450)
        );

        /**
         * mainDataStream为拆分出薪资小于10000的数据集
         */
        SingleOutputStreamOperator<Tuple2<String,Integer>> mainDataStream = dataStreamSource
                .process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void processElement(Tuple2<String, Integer> stringIntegerTuple2,
                                       Context context,
                                       Collector<Tuple2<String, Integer>> collector) throws Exception {

                if (stringIntegerTuple2.f1 > 10000){
                    context.output(outputTag,stringIntegerTuple2);
                } else {
                    collector.collect(stringIntegerTuple2);
                }

            }
        });

        /**
         * sideOutputStream为拆分出薪资大于10000的数据集
         */
        DataStream<Tuple2<String,Integer>> sideOutputStream = mainDataStream.getSideOutput(outputTag);

        sideOutputStream.print();

        /**
         * 6> (tony,33000)
         * 5> (pony,13000)
         * 2> (alex,11000)
         * 9> (jack,13450)
         */

        mainDataStream.print();
        /**
         * 13> (herry,4500)
         * 10> (lucy,3400)
         * 9> (lili,3200)
         * 14> (cherry,9000)
         */

        env.execute("stream");
    }
}
