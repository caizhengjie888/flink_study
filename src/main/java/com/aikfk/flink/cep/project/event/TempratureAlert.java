package com.aikfk.flink.cep.project.event;

public class TempratureAlert {

    private int rackID;

    public TempratureAlert(int rackID) {
        this.rackID = rackID;
    }

    public int getRackID() {
        return rackID;
    }

    public void setRackID(int rackID) {
        this.rackID = rackID;
    }

    @Override
    public String toString() {
        return "TempratureAlert{" +
                "rackID=" + rackID +
                '}';
    }
}
