package com.bigdata.flinkstudy.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Random;

/**
 * 处理窗口函数
 * 输出传感器的最高温度和最低温度
 */
public class ProcecssWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new SensorSource())
                .map(new MapFunction<String, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(String value) throws Exception {
                        final String[] values = value.split(",");
                        return Tuple2.of(Integer.parseInt(values[0]), Integer.parseInt(values[1]));
                    }
                })
                .keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<Integer, Integer>, String, Integer, TimeWindow>() {
                    /**
                     * 这里没有用到状态相关内容，仅仅体现获取窗口内的所有元素进行统计计算
                     * <IN, OUT, KEY, W extends Window>
                     */
                    @Override
                    public void process(Integer integer, Context context, Iterable<Tuple2<Integer, Integer>> iterable, Collector<String> collector) throws Exception {
                        int max = Integer.MIN_VALUE;
                        int min = Integer.MAX_VALUE;
                        int id = -1;

                        // 获取最大最小值
                        for (Tuple2<Integer, Integer> tuple2 : iterable) {
                            max = Math.max(max, tuple2.f1);
                            min = Math.min(min, tuple2.f1);
                            id = tuple2.f0;
                        }

                        // 输出结果
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        collector.collect("窗口区间：" + new Timestamp(start) + "~" + new Timestamp(end) +
                                "; ID：" + id + "; 最高温度：" + max + "; 最低温度：" + min);
                    }
                }).print();

        /**
         * 0,96,2022-04-12 14:51:52.008
         * 1,61,2022-04-12 14:51:53.012
         * 1,43,2022-04-12 14:51:54.018
         * 0,29,2022-04-12 14:51:55.023
         * 窗口区间：2022-04-12 14:51:50.0~2022-04-12 14:51:55.0; ID：0; 最高温度：96; 最低温度：96
         * 窗口区间：2022-04-12 14:51:50.0~2022-04-12 14:51:55.0; ID：1; 最高温度：61; 最低温度：43
         * 2,5,2022-04-12 14:51:56.026
         * 0,75,2022-04-12 14:51:57.028
         * 2,48,2022-04-12 14:51:58.032
         * 2,32,2022-04-12 14:51:59.033
         * 2,34,2022-04-12 14:52:00.034
         * 窗口区间：2022-04-12 14:51:55.0~2022-04-12 14:52:00.0; ID：0; 最高温度：75; 最低温度：29
         * 窗口区间：2022-04-12 14:51:55.0~2022-04-12 14:52:00.0; ID：2; 最高温度：48; 最低温度：5
         */

        env.execute();
    }
}

/**
 * 温度传感器数据源
 */
class SensorSource implements SourceFunction<String> {

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        final Random random = new Random();

        while (true) {
            long eventTime = System.currentTimeMillis();
            String msg = random.nextInt(3) + "," + random.nextInt(100);
            sourceContext.collectWithTimestamp(msg, eventTime);
            sourceContext.emitWatermark(new Watermark(eventTime - 1L));
            System.out.println(msg + "," + new Timestamp(eventTime));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {}
}