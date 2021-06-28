package com.flink.domain;

/**
 * WordWithCount
 *
 * @author: JWF
 * @date: 2021/6/28
 */
public class WordWithCount {
    public String word;
    public long count;

    public WordWithCount() {
    }

    public WordWithCount(String word, long count) {
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return word + " : " + count;
    }
}
