package com.bigdata.flinkstudy.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WebUIStreamWordCount {
    public static void main(String[] args) throws Exception {

        // 模拟入参(也可以在IDEA中设置运行时参数)
        args = new String[]{"-host", "localhost", "-port", "7777"};

        // 参数提取工具
        ParameterTool params = ParameterTool.fromArgs(args);

        // 1.创建"流"处理执行环境(带WEB-UI)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 2.创建 "流" 输入源。（Linux 要先执行 nc -lk 7777 启动一个socket窗口，然后在启动程序）
        DataStreamSource<String> dataStreamSource = env.socketTextStream(params.get("host"), params.getInt("port"));

        // 3.处理数据
        dataStreamSource
                // 这里用自定义类来代替lambda表达式，省去类型擦除的麻烦
                .flatMap(new Tokenizer())
                // 分组
                .keyBy(t -> t.f0)
                // 聚合
                .sum(1)
                // 打印（这里不会执行）
                .print();

        // 4.启动(流应用因为不会停止，所以需要一个启动的地方)
        env.execute();
    }

    /**
     * 自定义处理类
     */
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Long>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
            final String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(new Tuple2<>(word, 1L));
            }
        }
    }
}
