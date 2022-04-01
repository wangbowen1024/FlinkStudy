package com.bigdata.flinkstudy.operator.source;

import com.bigdata.flinkstudy.operator.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 *  用户自定义数据源（实现SourceFunction<T>接口，T是输出类型）
 *  如果需要支持并行度的数据源就要实现（ParallelSourceFunction接口，其余代码不变）
 */
public class MySource implements SourceFunction<Event> {
    private boolean isRunning = true;

    /**
     * 收集数据（这里模拟输出每秒输出一个对象）
     */
    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        String[] names = {"Tom", "Jerry", "Bob"};
        String[] paths = {"/home", "/user", "/login"};
        final Random random = new Random();

        while (isRunning) {
            sourceContext.collect(new Event(names[random.nextInt(names.length)],
                    paths[random.nextInt(paths.length)], System.currentTimeMillis()));
            Thread.sleep(1000);
        }
    }

    /**
     * 停止任务时候被调用，可以用于释放资源等清理工作
     */
    @Override
    public void cancel() {
        isRunning = false;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 如果是支持并行度的数据源，还可以设置并行度setParallelism(N)，不支持的设置1以上会报错
        env.setParallelism(1);

        // 添加自定义数据源
        DataStreamSource<Event> myStream = env.addSource(new MySource());
        myStream.print();

        env.execute();
    }
}
