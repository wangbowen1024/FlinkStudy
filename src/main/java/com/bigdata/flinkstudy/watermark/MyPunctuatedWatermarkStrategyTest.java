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
 * 自定义定点水位线生成器
 */
public class MyPunctuatedWatermarkStrategyTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new MyPunctuatedWatermarkStrategyTestSource())
                .assignTimestampsAndWatermarks(new MyPunctuatedWatermarkStrategy())
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
 * 自定义定点水位线生成器（接受到user=1时，才发出水位线）
 */
class MyPunctuatedWatermarkStrategy implements WatermarkStrategy<Event> {

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

            /**
             * 每次接受事件，遇到user=1时，将事件时间作为水位线发出
             */
            @Override
            public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
                if ("1".equals(event.getUser())) {
                    output.emitWatermark(new Watermark(eventTimestamp));
                }
            }

            /**
             * 周期性调用，不做任何处理
             */
            @Override
            public void onPeriodicEmit(WatermarkOutput output) {}
        };
    }
}


class MyPunctuatedWatermarkStrategyTestSource implements SourceFunction<Event> {
@Override
public void run(SourceContext<Event> sourceContext) throws Exception {
        // 窗口1：0～5秒，窗口2：5～10秒。

        sourceContext.collect(new Event("1", "/1", 1000L));
        // 【窗口1】事件时间1秒，水位线1秒
        sourceContext.collect(new Event("2", "/1", 3000L));
        // 【窗口1】事件时间3秒，水位线1秒
        sourceContext.collect(new Event("2", "/1", 6000L));
        // 【窗口2】事件时间6秒，水位线1秒
        sourceContext.collect(new Event("1", "/1", 2000L));
        // 【窗口1】事件时间2秒，水位线2秒
        sourceContext.collect(new Event("1", "/1", 7000L));
        // 【窗口2】事件时间7秒，水位线7秒。（触发计算窗口1，<1,2>、<2,1>）
        }

    @Override
    public void cancel() {}
}
