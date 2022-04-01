package com.bigdata.flinkstudy.operator.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 过滤（FilterFunction）
 * 符合条件的输出（true），否则丢弃
 */
public class FilterTransform {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> elementsDss = env.fromElements(1, 2, 3, 4);
        // 过滤奇数
        elementsDss.filter(x -> x % 2 == 0).print();

        env.execute();
    }
}
