package com.bigdata.flinkstudy.partition;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 全局分区（即合并到1个分区）
 */
public class GlobalPartition {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 自定义并行数据源
        env.addSource(new RichParallelSourceFunction<Integer>() {

            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                for (int i = 1; i <= 4; i++) {
                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                        sourceContext.collect(i);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2).global().print().setParallelism(4); // 这里并行度可以说没有用了

        env.execute();
    }
}
