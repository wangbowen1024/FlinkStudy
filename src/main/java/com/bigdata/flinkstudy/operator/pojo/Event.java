package com.bigdata.flinkstudy.operator.pojo;

import java.sql.Timestamp;
import java.util.Date;

/**
 * POJO 类
 * 1.共有类（public）
 * 2.一个无参构造方法
 * 3.所有属性都是公有（public）的
 * 4.所有属性的类型都是可以序列化的
 */
public class Event {
    public String user;
    public String url;
    public Long timestamp;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
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
