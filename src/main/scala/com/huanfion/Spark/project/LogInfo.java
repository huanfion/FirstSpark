package com.huanfion.Spark.project;

import java.io.Serializable;

/**
 * LogInfo
 * 自定义可序列化类
 */
public class LogInfo implements Serializable {
    private long timestamp;
    private long upTraffic;
    private long downTraffic;

    public LogInfo() {
    }

    public LogInfo(long timestamp,long upTraffic, long downTraffic) {
        this.timestamp = timestamp;
        this.upTraffic = upTraffic;
        this.downTraffic = downTraffic;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getUpTraffic() {
        return upTraffic;
    }

    public void setUpTraffic(long upTraffic) {
        this.upTraffic = upTraffic;
    }

    public long getDownTraffic() {
        return downTraffic;
    }

    public void setDownTraffic(long downTraffic) {
        this.downTraffic = downTraffic;
    }

}
