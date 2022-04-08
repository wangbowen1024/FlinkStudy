package com.bigdata.flinkstudy.watermark;

import com.bigdata.flinkstudy.operator.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * 水位线策略（flink内置）
 */
public class FlinkWatermarkStrategy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 周期性调用
        //env.getConfig().setAutoWatermarkInterval(200);

        env.fromElements(
                new Event("Tom1", "/home", 1000L),
                new Event("Tom2", "/login", 2000L),
                new Event("Tom3", "/opt", 3000L)
        )
                // 有序流的水位线生成，时间戳不会回退（直接拿当前最大的时间戳作为水位线）
                // 看生成器的代码可以知道，等价于：WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(0))
                /*.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()                           // 设置水位线
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {          // 设置时间戳（单位ms）
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.getTimestamp();
                            }
                        })
                );*/

                // 无序流的水位线生成（当前数据流中最大的时间戳减去延迟的时间）[唯一的区别就是比上面多了一个延迟时间2秒]
                // 乱序流中生成的水位线真正的时间戳，其实是 当前最大时间戳 – 延迟时间 – 1，这里的单位是毫秒。(因为时间>=t，考虑没有延迟临界情况)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))      // 设置水位线延迟时间
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {   // 设置时间戳
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.getTimestamp();
                                    }
                                })
                ).print();

        env.execute();
    }
}














