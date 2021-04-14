package com.aikfk.flink.base;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/7 12:55 下午
 */
public class WordCountPOJO {
    public String word;
    public int count;

    public WordCountPOJO() {
    }

    public WordCountPOJO(String word, int count) {
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
