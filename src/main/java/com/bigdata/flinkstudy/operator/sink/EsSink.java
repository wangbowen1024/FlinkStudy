package com.bigdata.flinkstudy.operator.sink;

import com.bigdata.flinkstudy.operator.pojo.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * 数据写入 ES
 */
public class EsSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> elementsDss = env.fromElements(
                new Event("Tom1", "/home", 1000L),
                new Event("Tom2", "/login", 2000L),
                new Event("Tom3", "/opt", 3000L)
        );

        // es host、数据处理 配置
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200));
        ElasticsearchSink.Builder<Event> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts, new MyElasticsearchSinkFunction());

        // 用户名密码配置（如果没有用户名密码这部分不用写）
        RestClientFactory restClientFactory = new RestClientFactory() {
            @Override
            public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY,
                        // 这里配置用户名、密码
                        new UsernamePasswordCredentials("elastic", "elastic"));
                restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        httpAsyncClientBuilder.disableAuthCaching();
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
            }
        };
        esSinkBuilder.setRestClientFactory(restClientFactory);
        // 一次批处理的数量
        esSinkBuilder.setBulkFlushMaxActions(1);
        // 批处理最大间隔时间(ms)
        esSinkBuilder.setBulkFlushInterval(1000);

        // ES数据汇构建，并添加
        elementsDss.addSink(esSinkBuilder.build());

        env.execute();
    }
}

/**
 * 自定义导入ES数据处理函数
 */
class MyElasticsearchSinkFunction implements ElasticsearchSinkFunction<Event> {

    @Override
    public void process(Event event, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
        HashMap<String, Object> result = new HashMap<>();
        result.put("user", event.getUser());
        result.put("url", event.getUrl());
        result.put("timestamp", event.getTimestamp());

        IndexRequest request = Requests.indexRequest()
                .index("clicks")        // 索引
                .source(result)         // 数据
                .id(event.getUser());   // ID值

        requestIndexer.add(request);
    }
}