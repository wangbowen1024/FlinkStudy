package com.bigdata.flinkstudy.partition;

import com.bigdata.flinkstudy.operator.pojo.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义分区
 */
public class MyPartition {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(
                new Event("1", "/home", 5000L),
                new Event("22", "/home", 5000L),
                new Event("333", "/home", 5000L)
        )
                // 参数1：根据KEY选择分区号
                // 参数2：根据传入的值自定义KEY
                .partitionCustom(new Partitioner<String>() {
                    @Override
                    public int partition(String s, int i) {
                        return s.length() % 4;
                    }
                }, new KeySelector<Event, String>() {
                    @Override
                    public String getKey(Event event) throws Exception {
                        return event.getUser();
                    }
                }).print().setParallelism(4);

        /**
         * 输出结果
         *
         * 4> Event{user='333', url='/home', timestamp=1970-01-01 08:00:05.0}
         * 2> Event{user='1', url='/home', timestamp=1970-01-01 08:00:05.0}
         * 3> Event{user='22', url='/home', timestamp=1970-01-01 08:00:05.0}
         *
         * 分析：
         *   1.首先，对传入对的 Event 对象定义KEY（取user字段作为KEY）
         *   2.其次，根据传入的KEY进行分区，这里采用获取字符串长度取余的方法
         *   3.结果，由于 "1" 对4取余后等于1，最后显示 2> Event{user='1'...  (因为。分区编号从0开始，显示的从1开始，因此1+1=2)
         */

        env.execute();
    }
}
