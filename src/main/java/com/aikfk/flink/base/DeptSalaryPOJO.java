package com.aikfk.flink.base;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/8 7:40 下午
 */
public class DeptSalaryPOJO {
    public String deptId;
    public int salary;

    public DeptSalaryPOJO() {
    }

    public DeptSalaryPOJO(String deptId, int salary) {
        this.deptId = deptId;
        this.salary = salary;
    }

    @Override
    public String toString() {
        return "DeptSalaryPOJO{" +
                "deptId='" + deptId + '\'' +
                ", salary=" + salary +
                '}';
    }
}
