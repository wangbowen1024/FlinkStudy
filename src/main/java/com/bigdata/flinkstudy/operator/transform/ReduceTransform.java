package com.bigdata.flinkstudy.operator.transform;

import com.bigdata.flinkstudy.operator.pojo.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * 规约聚合（如果我们要实现复杂聚合就要使用reduce了）
 * 解释: 每当来一个数据，在上一聚合结果(因此这里有一个状态的概念)的基础上进行新的聚合操作。
 *   A1:    A1=B1
 *   A2:    B1+A2=B2
 *   A3:    B2+A3=B3
 *   ......
 */
public class ReduceTransform {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Tom1", "/home", 5000L));
        events.add(new Event("Tom2", "/opt", 2000L));
        events.add(new Event("Tom2", "/login", 4000L));
        events.add(new Event("Tom1", "/home", 4000L));
        events.add(new Event("Tom1", "/opt", 2000L));
        DataStreamSource<Event> collectionDss = env.fromCollection(events);

        // Q:求最大用户及其访问量
        // 1.将每个访问记录变成 (用户,1)
        collectionDss.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return Tuple2.of(event.getUser(), 1L);
            }
        })
                // 2.对用户进行分组(用户, <(用户,1),(用户,1),...>)
                .keyBy(data -> data.f0)
                // 3.聚合 (用户,[[[1+1]+1]+1])
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> v1, Tuple2<String, Long> v2) throws Exception {
                        return Tuple2.of(v1.f0, v1.f1 + v2.f1);
                    }
                })
                // 4.由于要取最大用户以及访问量，要把所有数据都放到一个组里面算，因此这里写死KEY，把他们都放到同一个组里
                .keyBy(data -> "key")
                // 5.每当有用户的访问量大于等于当前的访问量，取新的那个用户以及访问量
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> v1, Tuple2<String, Long> v2) throws Exception {
                        return v1.f1 > v2.f1 ? v1 : v2;
                    }
                })
                .print();

        env.execute();
    }
}
