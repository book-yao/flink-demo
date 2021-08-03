package com.flink.windows.domain;

/**
 * WordWithCount
 *
 * @author: JWF
 * @date: 2021/6/28
 */
public class WordWithCount {
    public String word;
    public long count;
    public long timestamp;

    public WordWithCount() {
    }

    public WordWithCount(String word, long count) {
        this.word = word;
        this.count = count;
    }

    public WordWithCount(String word, long count,long timestamp) {
        this.word = word;
        this.count = count;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return word + " : " + count;
    }
}
