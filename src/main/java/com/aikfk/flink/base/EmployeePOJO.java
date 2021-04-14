package com.aikfk.flink.base;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/8 3:23 下午
 */
public class EmployeePOJO {
    public String deptId;
    public String name;
    public int salary;

    public EmployeePOJO() {

    }

    public EmployeePOJO(String deptId, String name, int salary) {
        this.deptId = deptId;
        this.name = name;
        this.salary = salary;
    }

    @Override
    public String toString() {
        return "EmployeePOJO{" +
                "deptId='" + deptId + '\'' +
                ", name='" + name + '\'' +
                ", salary=" + salary +
                '}';
    }
}
