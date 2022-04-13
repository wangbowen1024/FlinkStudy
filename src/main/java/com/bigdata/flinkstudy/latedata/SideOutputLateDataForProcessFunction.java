package com.bigdata.flinkstudy.latedata;

import com.bigdata.flinkstudy.operator.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class SideOutputLateDataForProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        OutputTag<Event> outputTag = new OutputTag<Event>("late-event") {};

        final SingleOutputStreamOperator<Event> stream = env.addSource(new SideOutputLateDataSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }))
                .process(new ProcessFunction<Event, Event>() {
                    @Override
                    public void processElement(Event event, Context context, Collector<Event> collector) throws Exception {
                        // 如果事件时间小于水位线，判断为迟到数据发送到副输出
                        if (context.timestamp() < context.timerService().currentWatermark()) {
                            context.output(outputTag, event);
                        } else {
                            collector.collect(event);
                        }
                    }
                });

        stream.print();
        stream.getSideOutput(outputTag).print("late");

        env.execute();
    }
}
