package com.bigdata.flinkstudy.operator.source;

import com.bigdata.flinkstudy.operator.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * 基础数据源读取
 */
public class BaseSource {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2-1.从文件中读取数据
        DataStreamSource<String> fileDss = env.readTextFile("input/clicks.txt");
        fileDss.print("fileDss");

        // 2-2.从集合中读取数据
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Tom1", "/home", 1000L));
        events.add(new Event("Tom2", "/opt", 2000L));
        events.add(new Event("Tom3", "/root", 3000L));
        DataStreamSource<Event> collectionDss = env.fromCollection(events);
        collectionDss.print("collectionDss");

        // 2-3.从集合元素中国呢读取数据
        DataStreamSource<Event> elementsDss = env.fromElements(        );
        elementsDss.print("elementsDss");

        // 2-4.从Socket中读取数据（参考 com.bigdata.flinkstudy.wc.StreamWordCount）

        env.execute();
    }
}
