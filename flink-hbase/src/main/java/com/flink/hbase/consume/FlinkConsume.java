package com.flink.hbase.consume;

import com.flink.hbase.reader.HBaseSetResource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkConsume {

    public static void main(String[] args) throws Exception {
        final LocalStreamEnvironment evn = StreamExecutionEnvironment.createLocalEnvironment();

//        evn.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        final DataStream<Tuple2<String, String>> dataStream = evn.addSource(new HBaseSetResource());

        final DataStream<Tuple2<String, Long>> resultStream = dataStream.flatMap(new MyFlatMapFunction())
                .keyBy(tuple -> tuple.f0)
                .sum(1);

        resultStream.print().setParallelism(1);

        evn.execute("hbase job");
    }

    public static class MyFlatMapFunction implements FlatMapFunction<Tuple2<String,String>,Tuple2<String,Long>> {

        @Override
        public void flatMap(Tuple2<String, String> stringStringTuple2, Collector<Tuple2<String, Long>> collector) throws Exception {
            collector.collect(new Tuple2<String,Long>(stringStringTuple2.f1,1L));
        }
    }
}
