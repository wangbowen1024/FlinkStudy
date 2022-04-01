package com.bigdata.flinkstudy.operator.sink;

import com.bigdata.flinkstudy.operator.pojo.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * 输出到Kafka
 */
public class KafkaSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从Kafka读取数据
        // 控制台生产者：bin/kafka-console-producer.sh --broker-list hdp01.bigdata.com:9092 --topic kafka-stream
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hdp01.bigdata.com:9092,hdp03.bigdata.com:9092,hdp02.bigdata.com:9092");
        properties.put("group.id", "group-flinkstudy");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "latest");
        DataStreamSource<String> kafkaStream = env.addSource(
                new FlinkKafkaConsumer<String>("kafka-stream", new SimpleStringSchema(), properties));

        // 2.转换操作
        SingleOutputStreamOperator<String> result = kafkaStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                final String[] fields = s.split(",");
                return new Event(fields[0], fields[1], Long.valueOf(fields[2])).toString();
            }
        });

        // 3.输出到Kafka
        // 控制台消费者：bin/kafka-console-consumer.sh --bootstrap-server hdp01.bigdata.com:9092 --topic kafka-stream-flink --from-beginning
        result.addSink(new FlinkKafkaProducer<String>(
                "hdp01.bigdata.com:9092,hdp03.bigdata.com:9092,hdp02.bigdata.com:9092",  // blocker-list
                "kafka-stream-flink",     // topic
                new SimpleStringSchema()         // 简单字符序列化器
        ));

        env.execute();
    }
}
