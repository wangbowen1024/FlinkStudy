package com.bigdata.flinkstudy.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理单词统计（文件读取）
 *
 * 底层上使用的是DataSet API，这是批处理的。（逐渐被弃用）
 * 但是从 flink 1.12 之后，官方推荐都使用 DataStream API ，即流批一体。
 * 如果想执行批任务，那么只要在提交任务的时候，带一个参数指明是批处理即可：-Dexecution-runtime-mode=BATCH
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {

        // 1.获得流执行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.读取数据（这里从文件中获取）
        DataSource<String> dataStream = env.readTextFile("input/words.txt");

        // 3.处理数据(flagMap -> 一对多，输入一个元素，输出一组元素。这里示例为输出一个二元组，即KV对)
        dataStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            final String[] words = line.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG))   // 范型擦除可以显示声明类型（lambda表达式时候需要）
                // 对元组集合中元素的第一个值作为KEY，进行分组形成 (hello, <1,1,1>)
                .groupBy(0)
                // 对分组后的第二个值进行聚合 （hello, 3）
                .sum(1)
                // 4.打印结果
                .print();
    }
}
