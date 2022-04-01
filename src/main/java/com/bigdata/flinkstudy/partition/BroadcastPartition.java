package com.bigdata.flinkstudy.partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 广播（即每个分区都会收到数据）
 */
public class BroadcastPartition {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream = env.fromElements(1, 2);
        // 广播
        stream.broadcast().print().setParallelism(4);

        env.execute();
    }
}
