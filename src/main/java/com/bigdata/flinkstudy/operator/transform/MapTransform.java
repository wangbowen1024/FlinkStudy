package com.bigdata.flinkstudy.operator.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 映射（MapFunction）
 * 一对一操作（接受一个数据，输出一个数据）
 */
public class MapTransform {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> elementsDss = env.fromElements(1, 2, 3, 4);
        // 对于每个数据进行+1操作
        elementsDss.map(x -> x + 1).print();

        env.execute();
    }
}
