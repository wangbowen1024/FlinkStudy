package com.bigdata.flinkstudy.partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 随机分区
 */
public class ShufflePartition {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);

        stream.shuffle().print().setParallelism(4);

        env.execute();
    }
}
