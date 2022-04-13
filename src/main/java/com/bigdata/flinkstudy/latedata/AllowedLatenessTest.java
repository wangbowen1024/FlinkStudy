package com.bigdata.flinkstudy.latedata;

import com.bigdata.flinkstudy.operator.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 窗口算子延迟容忍度测试
 */
public class AllowedLatenessTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new AllowedLatenessTestSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }))
                .keyBy(data -> data.getUser())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 额外处理3秒的迟到数据
                .allowedLateness(Time.seconds(3))
                .process(new ProcessWindowFunction<Event, Integer, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Event> iterable, Collector<Integer> collector) throws Exception {
                        int sum = 0;
                        for (Event event : iterable) {
                            System.out.println(event);
                            sum++;
                        }
                        collector.collect(sum);
                    }
                }).print();

        env.execute();
    }
}



class AllowedLatenessTestSource implements SourceFunction<Event> {

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        sourceContext.collect(new Event("Tom", "/home", 1000L));
        Thread.sleep(1000);
        sourceContext.collect(new Event("Tom", "/home", 3000L));
        Thread.sleep(1000);
        sourceContext.collect(new Event("Tom", "/home", 5000L));
        Thread.sleep(1000);
        sourceContext.collect(new Event("Tom", "/home", 7000L));
        Thread.sleep(1000);
        sourceContext.collect(new Event("Tom", "/home", 4000L));
        Thread.sleep(1000);
        sourceContext.collect(new Event("Tom", "/home", 4000L));
    }

    @Override
    public void cancel() {}
}