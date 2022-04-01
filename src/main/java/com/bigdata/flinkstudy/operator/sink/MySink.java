package com.bigdata.flinkstudy.operator.sink;

import com.bigdata.flinkstudy.operator.pojo.Event;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 自定义数据汇（以HBase为例）
 */
public class MySink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> elementsDss = env.fromElements(
                new Event("Tom1", "/home", 1000L),
                new Event("Tom2", "/login", 2000L),
                new Event("Tom3", "/opt", 3000L)
        );

        elementsDss.addSink(new MyHBaseSinkFunction());

        env.execute();
    }
}

/**
 * 自定义简易HBASE数据汇
 */
class MyHBaseSinkFunction extends RichSinkFunction<Event> {

    public org.apache.hadoop.conf.Configuration hbaseConf;
    // HBase 连接
    public Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 设置配置（一般是把 hbase-site.xml 放到资源目录下）
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "hdp01.bigdata.com:2181,hdp02.bigdata.com:2181,hdp03.bigdata.com:2181");
        connection = ConnectionFactory.createConnection(hbaseConf);
    }

    @Override
    public void invoke(Event value, Context context) throws Exception {
        // 获得 table
        Table table = connection.getTable(TableName.valueOf("ns1:t1"));
        // 构建 ROW_KEY
        byte[] rowId = Bytes.toBytes(value.getUser());
        // 创建 put 对象，并添加(列、字段、值)信息
        Put put = new Put(rowId);
        byte[] f1 = Bytes.toBytes("f1");
        put.addColumn(f1, Bytes.toBytes("user"), Bytes.toBytes(value.getUser()));
        put.addColumn(f1, Bytes.toBytes("url"), Bytes.toBytes(value.getUrl()));
        put.addColumn(f1, Bytes.toBytes("timestamp"), Bytes.toBytes(value.getTimestamp()));
        // 执行插入
        table.put(put);
        // 关闭表
        table.close();
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}