package com.bigdata.flinkstudy.operator.sink;

import com.bigdata.flinkstudy.operator.pojo.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 输出到JDBC
 */
public class FlinkJdbcSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> elementsDss = env.fromElements(
                new Event("Tom1", "/home", 1000L),
                new Event("Tom2", "/login", 2000L),
                new Event("Tom3", "/opt", 3000L)
        );

        elementsDss.addSink(JdbcSink.sink(
                // SQL
                "INSERT INTO clicks(username, url, create_time) VALUES(?, ?, ?)",
                // 绑定参数
                ((preparedStatement, event) -> {
                    preparedStatement.setString(1, event.getUser());
                    preparedStatement.setString(2, event.getUrl());
                    preparedStatement.setLong(3, event.getTimestamp());
                }),
                // JDBC连接属性
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:postgresql://localhost:5432/bigdata?currentSchema=flinkstudy")
                        .withUsername("postgres")
                        .withPassword("wbw1024")
                        .withDriverName("org.postgresql.Driver")
                        .build()
        ));

        env.execute();
    }
}
