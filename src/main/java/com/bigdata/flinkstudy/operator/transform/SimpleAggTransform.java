package com.bigdata.flinkstudy.operator.transform;

import com.bigdata.flinkstudy.operator.pojo.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * 简单聚合
 * 由flink帮我们实现了，如sum、max、min等。
 * 但是要注意的是滚动聚合算子会为每个处理过的键值维持一个状态。由于，这些状态不会被自动清理，所以该算子只能用于键值域有限的流。
 */
public class SimpleAggTransform {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Tom1", "/home", 5000L));
        events.add(new Event("Tom2", "/opt", 2000L));
        events.add(new Event("Tom2", "/root", 5000L));
        events.add(new Event("Tom2", "/login", 4000L));
        DataStreamSource<Event> collectionDss = env.fromCollection(events);
        /**
         * 求用户最近一次访问记录(使用max)
         * 从结果可以看到，max返回第一次遇到的数据，只对max的字段进行更新，取最大值，其余字段不变
         */
        collectionDss.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.getUser();
            }
        }).max("timestamp")
                .print("max");

        /**
         * 使用 maxBy
         * 从结果可以看到，maxBy会将整个数据进行更新，取完整的最新一条的数据
         */
        collectionDss.keyBy(event -> event.getUser())
                .maxBy("timestamp")
                .print("maxBy");

        env.execute();
    }
}
