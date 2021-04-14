package com.aikfk.flink.base;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/8 7:57 下午
 */
public class DeptPOJO {
    public String deptId;
    public String name;

    public DeptPOJO() {

    }

    public DeptPOJO(String deptId, String name) {
        this.deptId = deptId;
        this.name = name;
    }

    @Override
    public String toString() {
        return "DeptPOJO{" +
                "deptId='" + deptId + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
