package com.bigdata.flinkstudy.operator.sink;

import com.bigdata.flinkstudy.operator.pojo.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * 输出到文本文件
 *
 * 旧版本方法过时了：dataStream.writeAsText("output/writeAsTextLogs").setParallelism(1);
 */
public class TextFileSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Event> elementsDss = env.fromElements(
                new Event("Tom1", "/home", 1000L),
                new Event("Tom2", "/home", 2000L),
                new Event("Tom3", "/home", 3000L),
                new Event("Tom4", "/home", 4000L),
                new Event("Tom5", "/home", 4000L),
                new Event("Tom6", "/home", 4000L),
                new Event("Tom7", "/home", 4000L),
                new Event("Tom8", "/home", 4000L)
        );

        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("./output"),
                new SimpleStringEncoder<>("UTF-8"))
                // 滚动策略（满足条件后产生新文件）
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                // 文件大小（达到该限制大小，开启新文件）
                                .withMaxPartSize(1024)
                                // 滚动间隔（每隔该时间间隔结束写入，开启新文件）
                                .withRolloverInterval(TimeUnit.MINUTES.toMicros(15))
                                // 不活跃的时间（如果在该时间内没有新的数据，那么结束文件写入）
                                .withInactivityInterval(TimeUnit.MINUTES.toMicros(15))
                                .build()
                )
                .build();

        // 追加写入！
        elementsDss.map(data -> String.join("|", data.getUser(), data.getUrl(), String.valueOf(data.getTimestamp())))
                .addSink(streamingFileSink);

        env.execute();
    }
}
