package com.aikfk.flink.datastream.event;

import com.aikfk.flink.base.Tools;
import com.sun.tools.javac.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

public class SlideEventTimeWindow {


    private final static  long windowSize = 10;
    /**
     * key , eventTime
     *
     *
     * watermark , windowStartTime ,windowEndTime
     * @param args
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> dataStream_source = env.socketTextStream("bigdata-pro-m07" ,
                9999);

        DataStream<Tuple5<Integer , String,String,String,String>> dataStream =
                dataStream_source.map(new MapFunction<String, Tuple3<String, Long , Integer >>() {
                    @Override
                    public Tuple3<String, Long , Integer > map(String line) throws Exception {

                        String[] words = line.split( ",");

                        return new Tuple3<String,Long,Integer>(words[0] , Long.parseLong(words[1]) , 1);
                    }
                })
                        .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Long, Integer>>() {
                            private long currentMaxTimestamp = 0L;
                            private final long maxOutOfOrderness = 5000L;
                            private long lastEmittedWatermark = -9223372036854775808L;
                            private long offset = 0L ;
                            Watermark watermark = null ;

                            @Override
                            @Nullable

                            public Watermark getCurrentWatermark() {
                                long potentialWM = this.currentMaxTimestamp - this.maxOutOfOrderness;
                                watermark = new Watermark(potentialWM);
                                if (potentialWM >= this.lastEmittedWatermark) {
                                    this.lastEmittedWatermark = potentialWM;
                                }
                                return new Watermark(lastEmittedWatermark);
                            }

                            @Override
                            public long extractTimestamp(Tuple3<String, Long, Integer> tuple3, long l) {
                                long timestamp = tuple3.f1;
                                long windowStartTime = timestamp - (timestamp - offset + windowSize * 1000) % windowSize * 1000;
                                long windowEndTime = windowStartTime + windowSize * 1000 ;

                                System.out.println(
                                        "\n水印："+ Tools.getMsToDate(watermark.getTimestamp()) +
                                                "\ntimestamp = "+Tools.getMsToDate(timestamp )+
                                                "\nmaxCurrent = "+Tools.getMsToDate(currentMaxTimestamp) +
                                                "\nstartTime =" +Tools.getMsToDate(windowStartTime) +
                                                "\nendTime = "+Tools.getMsToDate(windowEndTime)+
                                                "\n================================================"
                                );


                                currentMaxTimestamp = Math.max(timestamp , currentMaxTimestamp) ;

                                return currentMaxTimestamp;
                            }
                        })
//                        .setParallelism(1)
                        .keyBy(0)
//                .timeWindow(Time.seconds(10))
                        .window(SlidingEventTimeWindows.of(Time.seconds(windowSize) , Time.seconds(5)))

                        .trigger(MyEventTimeTrigger.create())

                        /**
                         * windowSize , headTime , lastTime , windowStartTime , windowEndTime
                         */
                        .apply(new WindowFunction<Tuple3<String, Long, Integer>,
                                Tuple5<Integer , String,String,String,String>, Tuple, TimeWindow>() {
                            @Override
                            public void apply(Tuple tuple, TimeWindow timeWindow,
                                              Iterable<Tuple3<String, Long, Integer>> iterable,
                                              Collector<Tuple5<Integer , String,String,String,String>> collector) throws Exception {
                                collector.collect(new Tuple5<Integer , String,String,String,String>(
                                        List.from(iterable).size(),
                                        Tools.getMsToDate(List.from(iterable).head.f1),
                                        Tools.getMsToDate(List.from(iterable).last().f1),
                                        Tools.getMsToDate(timeWindow.getStart()),
                                        Tools.getMsToDate(timeWindow.getEnd())
                                ));
                            }
                        });
//                .sum(2);

        dataStream.print();
        env.execute("evenTimeWatermark");


    }
}
