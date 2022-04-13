package com.bigdata.flinkstudy.processfunction;

import com.bigdata.flinkstudy.operator.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 处理函数-计时器（事件时间）
 */
public class EventTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new CustomSource())
                // 设置事件时间
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }))
                .keyBy(data -> 1)
                .process(new KeyedProcessFunction<Integer, Event, String>() {
                    @Override
                    public void processElement(Event event, Context context, Collector<String> collector) throws Exception {
                        // 这里可以直接使用 timestamp() 方法，是因为上面的设置事件事件赋值了，处理事件例子中这样用会报错
                        collector.collect("数据到达时间(事件事件)：" + new Timestamp(context.timestamp()));
                        collector.collect("水位线：" + context.timerService().currentWatermark());
                        // 注册10秒后的计时器
                        context.timerService().registerEventTimeTimer(context.timestamp() + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("计时器触发！触发时间：" + new Timestamp(timestamp));
                    }
                })
                .print();

        /**
         * 第一条数据直接发出：事件时间是1秒，当前水位线是 Long.MIN_VALUE（设置计时器：11秒）
         * 第二条数据发出：事件时间是 11秒，当前水位线是 1秒-1ms=999ms（因此这时候不会触发计时器）
         * 第三条数据发出：事件时间11秒+1ms，当前水位线是 11秒-1ms=10999ms，该事件处理后水位线变成 11000ms（这时候触发第一个数据的计时器）
         * 最后5秒后输出剩下两个计时器：因为程序结束，时间直接拉到最大值 Long.MAX_VALUE，直接触发剩下2个数据的计时器
         *
         * 数据到达时间(事件事件)：1970-01-01 08:00:01.0
         * 水位线：-9223372036854775808
         * 数据到达时间(事件事件)：1970-01-01 08:00:11.0
         * 水位线：999
         * 数据到达时间(事件事件)：1970-01-01 08:00:11.001
         * 水位线：10999
         * 计时器触发！触发时间：1970-01-01 08:00:11.0
         * 计时器触发！触发时间：1970-01-01 08:00:21.0
         * 计时器触发！触发时间：1970-01-01 08:00:21.001
         */

        env.execute();
    }
}

/**
 * 自定义数据源
 */
class CustomSource implements SourceFunction<Event> {

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        // 立即发送一条1秒时间数据
        sourceContext.collect(new Event("1", "/1", 1000L));
        // 间隔5秒
        Thread.sleep(5000L);

        // 立即发送一条11秒时间数据（10秒后）
        sourceContext.collect(new Event("1", "/1", 11000L));
        // 间隔5秒
        Thread.sleep(5000L);

        // 发出 11 秒+1ms（10秒+1ms后）的数据
        sourceContext.collect(new Event("1", "/1", 11001L));
        // 间隔5秒
        Thread.sleep(5000L);
    }

    @Override
    public void cancel() {}
}