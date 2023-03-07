package com.yingzi.chapter05.Source;

import java.sql.Timestamp;

/**
 * @author 影子
 * @create 2022-04-12-16:47
 **/
public class Event {
    public String user;
    public String url;
    public Long timestamp;

    public Event(){
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
