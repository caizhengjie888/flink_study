package com.aikfk.flink.datastream.event;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * @author ：caizhengjie
 * @description：TODO
 * 需求：有订单数据，格式为:（订单ID，用户ID，时间戳/事件时间，订单金额）要求每隔5s，计算5秒内，每个用户的订单总金额
 * 并添加Watermaker来解决一定程度上的数据延迟和数据乱序问题。
 * @date ：2021/3/13 8:31 下午
 */
public class WatermakerDemo02 {
    public static void main(String[] args) throws Exception {

        FastDateFormat df = FastDateFormat.getInstance("HH:mm:ss");

        // env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // source：模拟实时订单数据(数据有延迟和乱序)
        DataStream<Order> orderDS = env.addSource(new SourceFunction<Order>() {

            private boolean flag = true;
            @Override
            public void run(SourceContext<Order> sourceContext) throws Exception {
                Random random = new Random();
                while (flag){
                    String orderId = UUID.randomUUID().toString();
                    int userId = random.nextInt(2);
                    int money = random.nextInt(101);
                    // 模拟数据延迟和乱序
                    long eventTime = System.currentTimeMillis() - random.nextInt(5) * 1000L;
                    System.out.println("发送的数据为: "+userId + " : " + df.format(eventTime));
                    sourceContext.collect(new Order(orderId,userId,money,eventTime));

                    Thread.sleep(1000);

                }
            }

            @Override
            public void cancel() {
                flag = false;

            }
        });

        /**
         * transformation:每隔5s计算最近5s的数据求每个用户的订单总金额,要求:基于事件时间进行窗口计算+Watermaker
         * env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); 在新版本中默认就是EventTime
         * 设置Watermaker = 当前最大的事件时间 - 最大允许的延迟时间或乱序时间
         *
         *         DataStream<Order>  orderDSWithWatermark = orderDS.assignTimestampsAndWatermarks(WatermarkStrategy
         *                 // 设置最大允许的延迟时间
         *                 .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
         *                 // 指定事时间件列
         *                 .withTimestampAssigner((event, timestamp) -> event.eventTime));
         *    开发中直接使用上面的即可
         */

        DataStream<Order> watermakerDS = orderDS.assignTimestampsAndWatermarks(new WatermarkStrategy<Order>() {
            @Override
            public WatermarkGenerator<Order> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<Order>() {

                    private int userId = 0;
                    private long eventTime = 0L;
                    private final long outOfOrdernessMillis = 3000;
                    private long maxTimestamp = Long.MAX_VALUE + outOfOrdernessMillis + 1;

                    @Override
                    public void onEvent(Order event, long eventTimestamp, WatermarkOutput output) {
                        userId = event.userId;
                        eventTime = event.eventTime;
                        maxTimestamp = Math.max(maxTimestamp,eventTimestamp);
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {

                        // Watermaker = 当前最大事件时间 - 最大允许的延迟时间或乱序时间
                        Watermark watermark = new Watermark(maxTimestamp - outOfOrdernessMillis - 1);
                        System.out.println("key:" + userId
                                + ",系统时间:" + df.format(System.currentTimeMillis())
                                + ",事件时间:" + df.format(eventTime)
                                + ",水印时间:" + df.format(watermark.getTimestamp()));

                        output.emitWatermark(watermark);

                    }
                };
            }
        }.withTimestampAssigner((order , timestamp) -> order.getEventTime()));

        /**
         * 接下来就可以进行窗口计算了,要求每隔5s,计算5秒内(基于时间的滚动窗口)，每个用户的订单总金额
         *  DataStream<Order> result = watermakerDS.keyBy(order -> order.userId)
         *                 .window(TumblingEventTimeWindows.of(Time.seconds(5)))
         *                 .sum("money");
         *  开发中使用上面的代码进行业务计算即可
         *  学习测试时可以使用下面的代码对数据进行更详细的输出,如输出窗口触发时各个窗口中的数据的事件时间,Watermaker时间
         *
         */
        DataStream<String> result = watermakerDS.keyBy(order -> order.userId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 把apply中的函数应用在窗口中的数据上
                // WindowFunction<IN, OUT, KEY, W extends Window>
                .apply(new WindowFunction<Order, String, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer key, TimeWindow window, Iterable<Order> orders, Collector<String> collector) throws Exception {
                        // 用来存放当前窗口的数据的格式化后的事件时间
                        List<String> list = new ArrayList<>();
                        for (Order order : orders){
                            Long eventTime = order.eventTime;
                            String formatEventTime = df.format(eventTime);
                            list.add(formatEventTime);
                        }
                        String start = df.format(window.getStart());
                        String end = df.format(window.getEnd());
                        // 现在就已经获取到了当前窗口的开始和结束时间,以及属于该窗口的所有数据的事件时间,把这些拼接并返回
                        String outStr = String.format("key:%s,窗口开始结束:[%s~%s),属于该窗口的事件时间:%s", key.toString(), start, end, list.toString());
                        collector.collect(outStr);

                    }
                });

        // print
        result.print();

        // execute
        env.execute("watermark");

    }

    /**
     * POJO类
     */
    public static class Order{
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long eventTime;

        public Order() {

        }

        public String getOrderId() {
            return orderId;
        }

        public void setOrderId(String orderId) {
            this.orderId = orderId;
        }

        public Integer getUserId() {
            return userId;
        }

        public void setUserId(Integer userId) {
            this.userId = userId;
        }

        public Integer getMoney() {
            return money;
        }

        public void setMoney(Integer money) {
            this.money = money;
        }

        public Long getEventTime() {
            return eventTime;
        }

        public void setEventTime(Long eventTime) {
            this.eventTime = eventTime;
        }

        public Order(String orderId, Integer userId, Integer money, Long eventTime) {
            this.orderId = orderId;
            this.userId = userId;
            this.money = money;
            this.eventTime = eventTime;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "orderId='" + orderId + '\'' +
                    ", userId=" + userId +
                    ", money=" + money +
                    ", eventTime=" + eventTime +
                    '}';
        }
    }
}
