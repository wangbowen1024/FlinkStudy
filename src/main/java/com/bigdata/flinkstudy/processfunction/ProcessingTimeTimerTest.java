package com.bigdata.flinkstudy.processfunction;

import com.bigdata.flinkstudy.operator.pojo.Event;
import com.bigdata.flinkstudy.operator.source.MySource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * 处理函数-计时器（处理时间）
 */
public class ProcessingTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new MySource())
                // 这里强制分区1并行度，只是个测试
                .keyBy(data -> 1)
                .process(new KeyedProcessFunction<Integer, Event, String>() {
                    @Override
                    public void processElement(Event event, Context context, Collector<String> collector) throws Exception {
                        // 当前处理时间
                        long currTs = context.timerService().currentProcessingTime();
                        collector.collect("数据到达时间：" + new Date(currTs));
                        // 注册一个 10 秒后的定时器
                        context.timerService().registerProcessingTimeTimer(currTs + 1000 * 10);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("触发计时器！触发时间：" + new Date(timestamp));
                    }
                })
                .print();

        /**
         * 数据到达时间：Mon Apr 11 14:34:38 CST 2022
         * 数据到达时间：Mon Apr 11 14:34:39 CST 2022
         * 数据到达时间：Mon Apr 11 14:34:40 CST 2022
         * 数据到达时间：Mon Apr 11 14:34:41 CST 2022
         * 数据到达时间：Mon Apr 11 14:34:42 CST 2022
         * 数据到达时间：Mon Apr 11 14:34:43 CST 2022
         * 数据到达时间：Mon Apr 11 14:34:44 CST 2022
         * 数据到达时间：Mon Apr 11 14:34:45 CST 2022
         * 数据到达时间：Mon Apr 11 14:34:46 CST 2022
         * 数据到达时间：Mon Apr 11 14:34:47 CST 2022
         * 触发计时器！触发时间：Mon Apr 11 14:34:48 CST 2022
         * 数据到达时间：Mon Apr 11 14:34:48 CST 2022
         */

        env.execute();
    }
}
