package com.bigdata.flinkstudy.window;

import com.bigdata.flinkstudy.operator.pojo.Event;
import com.bigdata.flinkstudy.operator.source.MySource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 和ReduceFunction差不多，多了一些状态属性，支持更复杂的操作。前者输入输出类型必须一致。
 * 将Event对象timestamp字段看成是访问时间，求每个用户平均访问时间
 */
public class AggregateWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(
                new Event("22", "/home", 3000L),
                new Event("22", "/home", 5000L),
                new Event("333", "/home", 5000L)
        )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.getTimestamp();
                                    }
                                })
                ).keyBy(data -> data.getUser())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 范型：输入、累加器、输出
                .aggregate(new AggregateFunction<Event, Tuple3<String, Long, Integer>, Tuple2<String, Double>>() {
                    /**
                     * 初始化
                     */
                    @Override
                    public Tuple3<String, Long, Integer> createAccumulator() {
                        return Tuple3.of("", 0L, 0);
                    }

                    /**
                     * 每个数据进来的时候都会调用，对累加器进行更新
                     */
                    @Override
                    public Tuple3<String, Long, Integer> add(Event value, Tuple3<String, Long, Integer> accumulator) {
                        return Tuple3.of(value.getUser(), accumulator.f1 + value.getTimestamp(), accumulator.f2 + 1);
                    }

                    /**
                     * 返回结果
                     */
                    @Override
                    public Tuple2<String, Double> getResult(Tuple3<String, Long, Integer> accumulator) {
                        return Tuple2.of(accumulator.f0, accumulator.f1 / Double.parseDouble(accumulator.f2.toString()));
                    }

                    /**
                     * 合并两个窗口的累加器（一般会话窗口时候）
                     */
                    @Override
                    public Tuple3<String, Long, Integer> merge(Tuple3<String, Long, Integer> a, Tuple3<String, Long, Integer> b) {
                        return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2);
                    }

                })
                .print();

        env.execute();
    }
}
