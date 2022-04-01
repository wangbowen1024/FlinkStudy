package com.bigdata.flinkstudy.operator.sink;

import com.bigdata.flinkstudy.operator.pojo.Event;
import com.bigdata.flinkstudy.operator.source.MySource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * 输出到redis
 */
public class FlinkRedisSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 自定义输入源（每秒随机输出用户名以及URI）
        DataStreamSource<Event> elementsDss = env.addSource(new MySource());

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .build();

        // 保存用户最后一次访问的URI，输出redis（接受两个参数，一个是配置主要是set方法，第二个是mapper接口）
        elementsDss.addSink(new RedisSink<>(conf, new MyRedisMapper()));

        env.execute();
    }
}

/**
 * 自定义 RedisMapper 实现类
 */
class MyRedisMapper implements RedisMapper<Event> {

    @Override
    public RedisCommandDescription getCommandDescription() {
        // 存入 HashSet 类型，表名叫 clicks
        return new RedisCommandDescription(RedisCommand.HSET, "clicks");
    }

    @Override
    public String getKeyFromData(Event event) {
        // 用户名作为KEY
        return event.getUser();
    }

    @Override
    public String getValueFromData(Event event) {
        // URI作为VALUE
        return event.getUrl();
    }
}