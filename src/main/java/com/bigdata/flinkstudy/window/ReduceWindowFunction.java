package com.bigdata.flinkstudy.window;

import com.bigdata.flinkstudy.operator.pojo.Event;
import com.bigdata.flinkstudy.operator.source.MySource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 窗口算子=窗口分配器+窗口函数
 *
 * 旧版本 timeWindow() 被弃用了
 */
public class ReduceWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new MySource())
        .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.getTimestamp();
                            }
                        })
        ).map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {
                return Tuple2.of(value.getUser(), 1L);
            }
        }).keyBy(data -> data.f0)
                /**窗口分配器*/
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))                   // 滚动窗口
                //.window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(15)))  // 滑动窗口
                //.window(EventTimeSessionWindows.withGap(Time.minutes(1)))             // 会话窗口
                //.countWindow(100)        // 计数窗口（一个参数就是滚动，两个参数就是滑动）
                /**增量聚合函数（输入输出只能相同类型）*/
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value2.f0, value1.f1 + value2.f1);
                    }
                }).print();

        env.execute();
    }
}














