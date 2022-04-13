package com.bigdata.flinkstudy.window;

import com.bigdata.flinkstudy.operator.pojo.Event;
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

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 默认触发器（其实就是对窗口最大时间戳进行注册和删除）
 */
public class DefaultTriggerTest extends Trigger<Event, TimeWindow> {

    /**
     * 每个元素添加窗口时调用
     */
    @Override
    public TriggerResult onElement(Event event, long l, TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println(window);
        System.out.println("onElement水位线：" + ctx.getCurrentWatermark());
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            System.out.println("onElement 执行");
            return TriggerResult.FIRE;
        } else {
            System.out.println("注册触发器：" + new Timestamp(window.maxTimestamp()));
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
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
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext triggerContext) throws Exception {
        System.out.println("触发计时器：(" + (time == window.maxTimestamp()) + ")" + new Timestamp(time));
        return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("删除触发器：" + new Timestamp(window.maxTimestamp()));
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new TriggerSource2())
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
                .trigger(new DefaultTriggerTest())
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

class TriggerSource2 implements SourceFunction<Event> {

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        sourceContext.collect(new Event("Tom", "/home", 1000L));
        Thread.sleep(1000);
        sourceContext.collect(new Event("Tom", "/home", 3000L));
        Thread.sleep(1000);
        sourceContext.collect(new Event("Tom", "/home", 5000L));
    }

    @Override
    public void cancel() {}
}