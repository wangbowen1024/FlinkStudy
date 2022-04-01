package com.bigdata.flinkstudy.partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 重缩放分区
 * 这种分区器会根据上下游算子的并行度，循环的方式输出到下游算子的每个实例。这里有点难以理解，
 * 假设上游并行度为 2，编号为 A 和 B。下游并行度为 4，编号为 1，2，3，4。那么 A 则把数据循环发送给 1 和 2，B 则把数据循环发送给 3 和 4。
 * 假设上游并行度为 4，编号为 A，B，C，D。下游并行度为 2，编号为 1，2。那么 A 和 B 则把数据发送给 1，C 和 D 则把数据发送给2。
 */
public class RescalePartition {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 自定义并行数据源
        env.addSource(new RichParallelSourceFunction<Integer>() {

            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                for (int i = 1; i <= 8; i++) {
                    /**
                     * 因为外面并行任务设置2，所以会有2个子任务，编号分别为0和1。
                     * 两个子任务都会读取8个数字，但是对于编号0的任务只输出偶数，对于编号1的任务只输出奇数。
                     */
                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                        sourceContext.collect(i);
                    }
                    /**
                     * 结果分析
                     *
                     * 数字（并行度2）       分组（并行度4）
                     * 1                   3
                     * 5                   3
                     *                     ------
                     * 3                   4
                     * 7                   4
                     *---------------------------
                     * 2                   1
                     * 6                   1
                     *                     ------
                     * 4                   2
                     * 8                   2
                     */
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2).rescale().print().setParallelism(4);

        env.execute();
    }
}
