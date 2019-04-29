package com.huanfion.Spark.project;

import scala.Serializable;
import scala.math.Ordered;

public class LogSort implements Ordered<LogSort>, Serializable {

    private long timeStamp;
    private long upTraffic;
    private long downTraffic;

    public LogSort() {
    }

    public LogSort(long timeStamp, long upTraffic, long downTraffic) {
        this.timeStamp = timeStamp;
        this.upTraffic = upTraffic;
        this.downTraffic = downTraffic;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
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

    @Override
    public int compare(LogSort that) {
        int comp=Long.valueOf(this.getUpTraffic()).compareTo((that.getUpTraffic()));
        if(comp==0){
            comp=Long.valueOf(this.getDownTraffic()).compareTo(that.getDownTraffic());
        }
        if(comp==0){
            comp=Long.valueOf(this.getTimeStamp()).compareTo(that.getTimeStamp());
        }
        return comp;
    }

    @Override
    public boolean $less(LogSort that) {
        return false;
    }

    @Override
    public boolean $greater(LogSort that) {
        return false;
    }

    @Override
    public boolean $less$eq(LogSort that) {
        return false;
    }

    @Override
    public boolean $greater$eq(LogSort that) {
        return false;
    }

    @Override
    public int compareTo(LogSort that) {
        int comp=Long.valueOf(this.getUpTraffic()).compareTo((that.getUpTraffic()));
        if(comp==0){
            comp=Long.valueOf(this.getDownTraffic()).compareTo(that.getDownTraffic());
        }
        if(comp==0){
            comp=Long.valueOf(this.getTimeStamp()).compareTo(that.getTimeStamp());
        }
        return comp;
    }
}
