package com.bigdata.flinkstudy.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 流处理单词统计
 *
 * 输出结果：
 * 3> (hello,1)
 * 3> (bigdata,1)
 * 3> (hello,2)
 * 3> (hello,3)
 * 7> (flink,1)
 * 5> (world,1)
 *
 * 解释：
 * 1.因为流应用接受数据是一条一条接受的，因此会输出多条记录
 * 2.因为这是分布式计算（本地用多线程模拟），因此输出会有乱序
 * 3.结果前面的数字其实是任务槽编号（默认任务槽个数=电脑核数。本机为8核，因此编号为0～7）
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {

        // 模拟入参(也可以在IDEA中设置运行时参数)
        args = new String[]{"-host", "localhost", "-port", "7777"};

        // 参数提取工具
        ParameterTool params = ParameterTool.fromArgs(args);

        // 1.创建"流"处理执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.创建 "流" 输入源。（Linux 要先执行 nc -lk 7777 启动一个socket窗口，然后在启动程序）
        DataStreamSource<String> dataStreamSource = env.socketTextStream(params.get("host"), params.getInt("port"));

        // 3.处理数据
        dataStreamSource
                // 这里用自定义类来代替lambda表达式，省去类型擦除的麻烦
                .flatMap(new Tokenizer())
                // 分组（对于元组下标为0的值的hashCode作为KEY）
                .keyBy(t -> t.f0)
                // 聚合（对于元组下标为1的值进行聚合）
                .sum(1)
                // 打印（这里不会执行）
                .print();

        // 4.启动(流应用因为不会停止，所以需要一个启动的地方)
        env.execute();

        /**
         * 输出结果：
         * 3> (hello,1)
         * 3> (bigdata,1)
         * 3> (hello,2)
         * 3> (hello,3)
         * 7> (flink,1)
         * 5> (world,1)
         */
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
