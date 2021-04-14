package com.aikfk.flink.cep.project.event;

import org.apache.hadoop.io.serializer.Serialization;

import java.io.Serializable;

public abstract class MonitorEvent {

    private int rackID ;
    private long currentTime ;

//    public MonitorEvent(int rackID, long currentTime) {
//        this.rackID = rackID;
//        this.currentTime = currentTime;
//    }

    public MonitorEvent(int rackID) {
        this.rackID = rackID;
    }

    public int getRackID() {
        return rackID;
    }

    public void setRackID(int rackID) {
        this.rackID = rackID;
    }

    public long getCurrentTime() {
        return currentTime;
    }

    public void setCurrentTime(long currentTime) {
        this.currentTime = currentTime;
    }
}
