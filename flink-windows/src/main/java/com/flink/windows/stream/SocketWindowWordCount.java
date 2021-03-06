package com.flink.windows.stream;

import com.flink.windows.domain.WordWithCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * SocketWindowWordCount
 *
 * @Description: flink单词个数统计 - windows安装netcat-win32-1.12（配置环境变量）后，cmd运行 nc -l -p 9000回车后，输入字符串
 * @author: JWf
 * @date: 2021-6-28
 */
public class SocketWindowWordCount {
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
        //获取执行环节
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取连接socket输入数据
        DataStream<String> text = env.socketTextStream(host, port, "\n");

        //解析数据、对数据进行分组、窗口函数和统计个数

        DataStream<WordWithCount> windowCounts = text.flatMap(
                new FlatMapFunction<String, WordWithCount>() {
                    private static final long serialVersionUID = 6800597108091365154L;

                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                        // 切割空白字符
                        for (String word : value.split(" ")) {
                            out.collect(new WordWithCount(word, 1));
                        }
                    }
                })
                // 按word分区
                .keyBy("word")
                // 定义 30 分钟超时的会话窗口
//                .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
                // 每隔10秒统计5分钟内的数据
//                .timeWindow(Time.minutes(5), Time.seconds(10))
                .reduce((value1, value2) -> new WordWithCount(value1.word, value1.count + value2.count));
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }

}
