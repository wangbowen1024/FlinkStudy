package com.bigdata.flinkstudy.partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 轮询分区
 */
public class RebalancePartition {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);


        stream.rebalance().print().setParallelism(4);

        /**
         * 一开始我们设置并行度为1，因此接受数据的并行度是1。而打印输出的并行度我们设置成4。
         * flink中对于并行度由少变多，默认采用轮训分区
         */
        //stream.print().setParallelism(4);

        env.execute();
    }
}
