package com.aikfk.flink.datastream.bean;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/20 9:19 下午
 * 水位传感器：用于接收水位数据
 * <p>
 * id:传感器编号
 * ts:时间戳
 * vc:水位
 */
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;

    public WaterSensor(String id, Long ts, Integer vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }
    public WaterSensor() {

    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Integer getVc() {
        return vc;
    }

    public void setVc(Integer vc) {
        this.vc = vc;
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                ", vc=" + vc +
                '}';
    }
}
