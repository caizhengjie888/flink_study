package com.aikfk.flink.cep.project.event;

public class TempratureWaring {
    private int rackID ;
    private double avgTemperature;

    public TempratureWaring(int rackID, double avgTemperature) {
        this.rackID = rackID;
        this.avgTemperature = avgTemperature;
    }

    public int getRackID() {
        return rackID;
    }

    public void setRackID(int rackID) {
        this.rackID = rackID;
    }

    public double getAvgTemperature() {
        return avgTemperature;
    }

    public void setAvgTemperature(double avgTemperature) {
        this.avgTemperature = avgTemperature;
    }
}
