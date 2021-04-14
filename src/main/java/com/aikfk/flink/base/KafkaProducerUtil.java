package com.aikfk.flink.base;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class KafkaProducerUtil extends Thread {

        private String topic = "kfk";

        public KafkaProducerUtil() {
            super();
        }

        private Producer<String, String> createProducer() {
            // 通过Properties类设置Producer的属性
            Properties properties = new Properties();
            properties.put("bootstrap.servers", "bigdata-pro-m07:9092");
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            return new KafkaProducer<String, String>(properties);
        }

        @Override
        public void run() {
            Producer<String, String> producer = createProducer();
            Random random = new Random();
            Random random2 = new Random();

            while (true) {
                String user_id = "user_"+random.nextInt(10);
                String product_id = "product_"+random2.nextInt(100);
                System.out.println(user_id + " :" + product_id);
                String time = System.currentTimeMillis() / 1000 + 5 + "";
                try {
//
                    String kaifa_log = "{" +
                            "\"user_id\":\"" + user_id+"\"," +
                            "\"product_id\":\"" + product_id+"\"," +
                            "\"click_count\":\"1" + "\"," +
                            "\"ts\":" + time + "}";
                    System.out.println("kaifa_log = " + kaifa_log);
                    producer.send(new ProducerRecord<String, String>(this.topic, kaifa_log));

                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println("=========循环一次==========");


                try {
                    sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public static void main(String[] args) {
            new KafkaProducerUtil().run();
        }

}
