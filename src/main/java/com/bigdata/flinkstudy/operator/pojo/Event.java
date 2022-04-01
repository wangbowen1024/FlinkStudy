package com.bigdata.flinkstudy.operator.pojo;

import java.sql.Timestamp;

/**
 * POJO 类
 * 1.共有类（public）
 * 2.一个无参构造方法
 * 3.所有字段都要是public，或者有getter/setter方法
 * 4.所有属性的类型都是可以序列化的
 */
public class Event {
    private String user;
    private String url;
    private Long timestamp;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
