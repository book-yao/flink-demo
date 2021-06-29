package com.flink.kafka.consume;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 消费Kafka数据
 *
 * @author JWF
 * @date 2021-6-28
 */
public class ConsumeKafka {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //kafka配置
        String topic = "test";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "10.20.31.244:9092");
        prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("auto.offset.reset", "latest");
        prop.setProperty("group.id", "flink-consumer");

        final FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer(topic, new SimpleStringSchema(), prop);
        final DataStream<String> text = env.addSource(flinkKafkaConsumer);

        final DataStream<Tuple2<String, Long>> resultStream = text.flatMap(new MyFlatMapFunction())
                .keyBy((Tuple2<String, Long> tuple) -> tuple.f0)
                .sum(1);
        resultStream.print().setParallelism(2);

        env.execute("consume kafka");
    }

    public static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Long>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Long>> collector) throws Exception {
            final String[] s = value.split(" ");
            for (String word : s) {
                collector.collect(new Tuple2<String, Long>(word, 1L));
            }
        }
    }
}
