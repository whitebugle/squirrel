package com.jinghang.flink12.JTest;

public class Loginlog {
    String id;
    String username;
    String channel;
    Long time;

    public Loginlog() {
    }

    @Override
    public String toString() {
        return "Loginlog{" +
                "id='" + id + '\'' +
                ", username='" + username + '\'' +
                ", channel='" + channel + '\'' +
                ", time='" + time + '\'' +
                '}';
    }

    public Loginlog(String id, String username, String channel, Long time) {
        this.id = id;
        this.username = username;
        this.channel = channel;
        this.time = time;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }
}
