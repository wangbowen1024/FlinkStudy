package com.bigdata.flinkstudy.window;

import com.bigdata.flinkstudy.operator.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * 自定义删除器
 * 过滤前：
 *   (Tom,2)
 *   (Tom2,1)
 * 过滤后：(这里窗口还是存在的，只是里面的元素没了)
 *   (Tom,2)
 *   (Tom2,0)
 */
public class MyEvictor implements Evictor<Event, TimeWindow> {

    @Override
    public void evictBefore(Iterable<TimestampedValue<Event>> iterable, int i, TimeWindow window, EvictorContext evictorContext) {

        final Iterator<TimestampedValue<Event>> iterator = iterable.iterator();
        while (iterator.hasNext()) {
            if ("Tom2".equals(iterator.next().getValue().getUser())) {
                iterator.remove();
            }
        }
    }

    @Override
    public void evictAfter(Iterable<TimestampedValue<Event>> iterable, int i, TimeWindow window, EvictorContext evictorContext) {}

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(
                new Event("Tom", "/home", 1000L),
                new Event("Tom", "/home", 1000L),
                new Event("Tom2", "/home", 1000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }))
                .keyBy(data -> data.getUser())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .evictor(new MyEvictor())
                .process(new ProcessWindowFunction<Event, Tuple2<String, Integer>, String, TimeWindow>() {

                    @Override
                    public void process(String s, Context context, Iterable<Event> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        int count = 0;
                        for (Event event : iterable) {
                            count++;
                        }
                        collector.collect(Tuple2.of(s, count));
                    }
                }).print();

        env.execute();
    }
}
