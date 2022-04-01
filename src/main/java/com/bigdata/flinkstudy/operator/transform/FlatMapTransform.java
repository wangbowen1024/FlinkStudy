package com.bigdata.flinkstudy.operator.transform;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 扁平化映射（FlatMapFunction）
 * 先对一个整体进行拆分打散，然后对每个元素进行一对一操作（输入1个，输出多个）
 */
public class FlatMapTransform {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> elementsDss = env.fromElements(1, 2, 3);
        // 这里对于1加100后输出1次，对于2输出2次
        elementsDss.flatMap((Integer num, Collector<Integer> out) -> {
            if (num == 1) {
                out.collect(num + 100);
            } else if (num == 2) {
                out.collect(num);
                out.collect(num);
            }
        }).returns(Types.INT)  // lambda 范型擦除
          .print();

        env.execute();
    }
}
