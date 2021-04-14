package com.aikfk.flink.dataset.source;

import com.aikfk.flink.base.WordCountPOJO;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/7 11:54 上午
 */
public class JsonSourceJava {

    final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        // 准备环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取json数据源
        DataSet<WordCountPOJO> dataSet = env.readTextFile("/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/wordcount.json")
                .map(new MapFunction<String, WordCountPOJO>() {
                    @Override
                    public WordCountPOJO map(String line) throws Exception {
                        WordCountPOJO wordCountPOJO = OBJECT_MAPPER.readValue(line,WordCountPOJO.class);
                        return wordCountPOJO;
                    }
                })
                .groupBy("word")
                .reduce(new ReduceFunction<WordCountPOJO>() {
                    @Override
                    public WordCountPOJO reduce(WordCountPOJO t1, WordCountPOJO t2) throws Exception {
                        return new WordCountPOJO(t1.word,t1.count + t2.count);
                    }
                });

        dataSet.print();
    }
}
