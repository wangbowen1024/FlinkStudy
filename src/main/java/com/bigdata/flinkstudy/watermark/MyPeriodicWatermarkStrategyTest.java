package com.bigdata.flinkstudy.watermark;

import com.bigdata.flinkstudy.operator.pojo.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 自定义周期性水位线生成器
 */
public class MyPeriodicWatermarkStrategyTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new MyPeriodicWatermarkStrategyTestTest())
                .assignTimestampsAndWatermarks(new MyPeriodicWatermarkStrategy())
                .map(data -> Tuple2.of(data.getUser(), 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce((a, b) -> Tuple2.of(b.f0, a.f1 + b.f1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .print();

        env.execute();
    }
}

/**
 * 自定义周期性容忍度为2秒的水位线生成器
 */
class MyPeriodicWatermarkStrategy implements WatermarkStrategy<Event> {

    /**
     * 分配时间戳
     */
    @Override
    public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.getTimestamp();
            }
        };
    }

    /**
     * 水位线生成器
     */
    @Override
    public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<Event>() {

            // 容忍度2秒
            private long delay = 2000L;
            // 当前最大时间戳
            private long maxTs = Long.MIN_VALUE;

            /**
             * 每次接受事件，将时间戳更新
             */
            @Override
            public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
                maxTs = Math.max(maxTs, eventTimestamp);
            }

            /**
             * 周期性调用该方法，发出水位线
             */
            @Override
            public void onPeriodicEmit(WatermarkOutput output) {
                output.emitWatermark(new Watermark(maxTs - delay - 1));
            }
        };
    }
}

/**
 * 自定义数据源（滚动窗口）
 */
class MyPeriodicWatermarkStrategyTestTest implements SourceFunction<Event> {
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