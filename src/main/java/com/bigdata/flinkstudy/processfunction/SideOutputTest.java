package com.bigdata.flinkstudy.processfunction;

import com.bigdata.flinkstudy.operator.pojo.Event;
import com.bigdata.flinkstudy.operator.source.MySource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Date;

/**
 * 侧输出流
 */
public class SideOutputTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.声明一个侧输出流标签
        OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

        SingleOutputStreamOperator<String> stream = env.addSource(new MySource())
                // 这里强制分区1并行度，只是个测试
                .keyBy(data -> 1)
                .process(new KeyedProcessFunction<Integer, Event, String>() {
                    @Override
                    public void processElement(Event event, Context context, Collector<String> collector) throws Exception {
                        // 正常输出
                        collector.collect("正常流数据：" + event);
                        // 2.侧流输出（参数1：侧输出流标签；参数2：值）
                        context.output(outputTag, "侧输出流数据：" + event);
                    }

                });

        // 正常流输出
        stream.print();
        // 3.侧输出流操作（和普通的一样操作）
        stream.getSideOutput(outputTag).print();

        env.execute();
    }
}
