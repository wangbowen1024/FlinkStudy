package com.bigdata.flinkstudy.window;

import com.bigdata.flinkstudy.operator.pojo.Event;
import com.bigdata.flinkstudy.operator.source.MySource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * 自定义触发器(设置这个后，默认的触发器就失效了)
 * 可提前触发的触发器
 */
public class MyTrigger extends Trigger<Event, TimeWindow> {

    /**
     * 每个元素添加窗口时调用(这里l应该是当时间事件)
     */
    @Override
    public TriggerResult onElement(Event event, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        // 获取窗口状态，一开始 firstEvent 值为 false
        ValueState<Boolean> firstEvent = triggerContext.getPartitionedState(new ValueStateDescriptor<Boolean>("firstEvent", Boolean.class));
        // 仅为第一个元素注册初始计时器
        if (firstEvent.value() == null) {
            // 注册事件时间计时器1，将水位线上取整到秒来计算下一次触发时间
            long t = triggerContext.getCurrentWatermark() + (1000 - (triggerContext.getCurrentWatermark() % 1000));
            triggerContext.registerEventTimeTimer(t);
            // 为窗口结束时间注册计时器2
            triggerContext.registerEventTimeTimer(timeWindow.getEnd());
            // 更新状态值
            firstEvent.update(true);
        }
        // 继续。不会针对每个元素都计算
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        // 继续，我们不使用处理时间
        return TriggerResult.CONTINUE;
    }

    /**
     * 事件时间计时器触发时调用
     */
    @Override
    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        // 如果水位线=窗口结束时间（即，计时器2）
        if (l == timeWindow.getEnd()) {
            // 进行最终计算并清理窗口状态
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            // 注册下一个用于提前触发的计时器(相当于每秒一次了)
            long t = triggerContext.getCurrentWatermark() + (1000 - (triggerContext.getCurrentWatermark() % 1000));
            if (t < timeWindow.getEnd()) {
                triggerContext.registerEventTimeTimer(t);
            }
            // 触发进行窗口计算
            return TriggerResult.FIRE;
        }
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        // 清理触发器状态
        ValueState<Boolean> firstEvent = triggerContext.getPartitionedState(new ValueStateDescriptor<Boolean>("firstEvent", Boolean.class));
        firstEvent.clear();
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new TriggerSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.getTimestamp();
                                    }
                                })
                )
                .keyBy(data -> data.getUser())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(new MyTrigger())
                .aggregate(new AggregateFunction<Event, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

                    @Override
                    public Tuple2<String, Integer> createAccumulator() {
                        return Tuple2.of("", 0);
                    }

                    @Override
                    public Tuple2<String, Integer> add(Event value, Tuple2<String, Integer> accumulator) {
                        return Tuple2.of(value.getUser(), accumulator.f1 + 1);
                    }

                    @Override
                    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                        return Tuple2.of(a.f0, a.f1 + b.f1);
                    }
                })
                .print();

        env.execute();
    }
}

class TriggerSource implements SourceFunction<Event> {

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        sourceContext.collect(new Event("Tom", "/home", 1000L));
        Thread.sleep(1000);
        sourceContext.collect(new Event("Tom", "/home", 3000L));
        Thread.sleep(1000);
        sourceContext.collect(new Event("Tom", "/home", 5000L));
        Thread.sleep(1000);
        sourceContext.collect(new Event("Tom", "/home", 7000L));
        Thread.sleep(1000);
        sourceContext.collect(new Event("Tom", "/home", 10000L));
        Thread.sleep(10000000);
    }

    @Override
    public void cancel() {}
}