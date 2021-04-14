package com.aikfk.flink.base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/5 3:07 下午
 */
public class WordCountJava3 {
    public static void main(String[] args) throws Exception {

        // 准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<WordCount> dataStream = env.socketTextStream("bigdata-pro-m07",9999)
                .flatMap(new FlatMapFunction<String, WordCount>() {
                    @Override
                    public void flatMap(String line, Collector<WordCount> collector) throws Exception {
                        String[] words = line.split(" ");
                        for (String word : words){
                            collector.collect(new WordCount(word,1));
                        }
                    }
                })
                // 将相同key的value放在同一个partition（按照key选择器指定）
                .keyBy(new KeySelector<WordCount, Object>() {
                    @Override
                    public Object getKey(WordCount wordCount) throws Exception {
                        return wordCount.word;
                    }
                })
                .reduce(new ReduceFunction<WordCount>() {
                    @Override
                    public WordCount reduce(WordCount t1, WordCount t2) throws Exception {
                        return new WordCount(t1.word , t1.count + t2.count);
                    }
                });

        dataStream.print();

        env.execute("Window WordCount");
    }

    /**
     * POJO类
     */
    public static class WordCount{
        public String word;
        public int count;

        public WordCount() {
        }

        public WordCount(String word, int count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
