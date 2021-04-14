package com.aikfk.flink.cep.project.event;

public class TemperatureEvent extends MonitorEvent{

    private Double temperature;

    public TemperatureEvent(int rackID , Double temperature) {
        super(rackID);
        this.temperature = temperature;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "TemperatureEvent{" +
                "temperature=" + temperature +
                '}';
    }
}
