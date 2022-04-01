package com.bigdata.flinkstudy.operator.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Kafka数据源读取数据
 */
public class KafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // kafka 配置
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hdp01.bigdata.com:9092,hdp03.bigdata.com:9092,hdp02.bigdata.com:9092");
        properties.put("group.id", "group-flinkstudy");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "latest");

        // 添加Kafka数据源（第一个参数：主题）
        DataStreamSource<String> kafkaStream = env.addSource(
                new FlinkKafkaConsumer<String>("kafka-stream", new SimpleStringSchema(), properties));

        kafkaStream.print();

        env.execute();
    }
}
