package com.aikfk.flink.sql.pojo;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/4/5 10:30 下午
 */
public class WC {

    public  String wordName ;
    public long freq;

    public WC() {
    }

    public WC(String wordName, long freq) {
        this.wordName = wordName;
        this.freq = freq;
    }

    public String getWordName() {
        return wordName;
    }

    public void setWordName(String wordName) {
        this.wordName = wordName;
    }

    public long getFreq() {
        return freq;
    }

    public void setFreq(long freq) {
        this.freq = freq;
    }

    @Override
    public String toString() {
        return "WC{" +
                "wordName='" + wordName + '\'' +
                ", freq=" + freq +
                '}';
    }
}
