package com.bigdata.flinkstudy.watermark;

import com.bigdata.flinkstudy.operator.pojo.Event;
import com.bigdata.flinkstudy.operator.source.MySource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 测试水位线更新以及窗口触发计算
 */
public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new MySourceTest())
        .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
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
                // 滚动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 增量聚合
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value2.f0, value1.f1 + value2.f1);
                    }
                }).print();


        env.execute();
    }
}

/**
 * 自定义数据源（滚动窗口）
 */
class MySourceTest implements SourceFunction<Event> {
    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        // 窗口1：0～5秒，窗口2：5～10秒。延迟2秒。

        sourceContext.collect(new Event("1", "/1", 1000L));
        // 【窗口1】事件时间1秒，最大时间戳1秒，水位线-1秒
        sourceContext.collect(new Event("1", "/1", 3000L));
        // 【窗口1】事件时间3秒，最大时间戳3秒，水位线1秒
        sourceContext.collect(new Event("1", "/1", 6000L));
        // 【窗口2】事件时间6秒，最大时间戳6秒，水位线4秒
        sourceContext.collect(new Event("1", "/1", 2000L));
        // 【窗口1】事件时间2秒，最大时间戳6秒，水位线4秒
        sourceContext.collect(new Event("1", "/1", 7000L));
        // 【窗口2】事件时间7秒，最大时间戳7秒，水位线5秒（触发窗口1计算，这时候应该输出<1，3>）
        Thread.sleep(1000*1000000);
        // 然后进行无限等待，水位线永远到不了10秒，因此第二个窗口不会触发计算
    }

    @Override
    public void cancel() {}
}
