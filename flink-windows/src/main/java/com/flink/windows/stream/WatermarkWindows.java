package com.flink.windows.stream;

import com.flink.windows.domain.WordWithCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author: jiangwf39668
 * @createDate: 2021/8/2 8:40
 */
public class WatermarkWindows {
    public static void main(String[] args) throws Exception {
        //连接端口号
        final int port;
        final String host;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            port = params.getInt("port");
            host = params.get("host");
        } catch (Exception e) {
            System.out.println("No port specified. Please run 'SocketWindowWordCount --port <port>'");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //获取连接socket输入数据
        DataStream<String> text = env.socketTextStream(host, port, "\n");

        OutputTag<WordWithCount> outputTag = new OutputTag<WordWithCount>("side"){

        };

        final SingleOutputStreamOperator<WordWithCount> windowsStream = text.flatMap(
                new FlatMapFunction<String, WordWithCount>() {
                    private static final long serialVersionUID = 6800597108091365154L;

                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                        final String[] split = value.split(",");
                        out.collect(new WordWithCount(split[0], Long.valueOf(split[1]), Long.valueOf(split[2])));
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<WordWithCount>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(WordWithCount wordWithCount) {
                        return wordWithCount.timestamp;
                    }
                })
                // 按word分区
                .keyBy("word")
                // 定义 30 分钟超时的会话窗口
//                .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
                // 每隔10秒统计5分钟内的数据
//                .timeWindow(Time.minutes(5), Time.seconds(10))
                .timeWindow(Time.seconds(15))
                // 允许延迟时间
                .allowedLateness(Time.seconds(30))
                // 侧输出流
                .sideOutputLateData(outputTag)
                .minBy("count");
        windowsStream.print("count");
        // 获取侧输出流
        windowsStream.getSideOutput(outputTag).print("outputTag");

        env.execute("Socket Window WordCount");
    }
}
