package com.aikfk.flink.datastream.event;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;

/**
 * @author ：caizhengjie
 * @description：TODO
 * 需求：有订单数据，格式为:（订单ID，用户ID，时间戳/事件时间，订单金额）要求每隔5s，计算5秒内，每个用户的订单总金额
 * 并添加Watermaker来解决一定程度上的数据延迟和数据乱序问题。
 * @date ：2021/3/13 8:31 下午
 */
public class WatermakerDemo01 {
    public static void main(String[] args) throws Exception {

        // env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // source
        DataStream<Order> orderDS = env.addSource(new SourceFunction<Order>() {

            private boolean flag = true;
            @Override
            public void run(SourceContext<Order> sourceContext) throws Exception {
                Random random = new Random();
                while (flag){
                    String orderId = UUID.randomUUID().toString();
                    int userId = random.nextInt(2);
                    int money = random.nextInt(101);
                    // 随机模拟延迟
                    long eventTime = System.currentTimeMillis() - random.nextInt(5) * 1000L;
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
         */
        SingleOutputStreamOperator<Order>  orderDSWithWatermark = orderDS.assignTimestampsAndWatermarks(WatermarkStrategy
                // 设置最大允许的延迟时间
                .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                // 指定事时间件列
                .withTimestampAssigner((event, timestamp) -> event.eventTime));

        SingleOutputStreamOperator<Order> result = orderDSWithWatermark.keyBy(order -> order.userId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("money");


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
