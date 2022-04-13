package com.bigdata.flinkstudy.window;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;
import java.util.Collections;

/**
 * 自定义窗口分配器
 * 将时间按照每30秒滚动窗口进行分组的自定义窗口
 */
public class MyWindowAssigner extends WindowAssigner<Object, TimeWindow> {

    private long windowSize = 30 * 1000L;

    /**
     * 分配窗口(这里返回的是一个列表，也就是说可以返回多个窗口)
     */
    @Override
    public Collection<TimeWindow> assignWindows(Object o, long ts, WindowAssignerContext windowAssignerContext) {
        // 30秒取余（确定窗口的开始、结束时间）
        long startTime = ts - (ts % windowSize);
        long endTime = startTime + windowSize;
        // 发出相应的时间窗口(Collections.singletonList()返回的是不可变的集合，但是这个长度的集合只有1)
        return Collections.singletonList(new TimeWindow(startTime, endTime));
    }

    /**
     * 获取默认触发器
     */
    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
        return EventTimeTrigger.create();
    }

    /**
     * 窗口类型序列化器
     */
    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}
