package com.aikfk.flink.dataset.transform;

import com.aikfk.flink.base.DeptPOJO;
import com.aikfk.flink.base.DeptSalaryPOJO;
import com.aikfk.flink.base.EmployeePOJO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/8 7:36 下午
 */
public class UnionJava {
    public static void main(String[] args) throws Exception {
        // 准备环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取csv数据（方式一：映射POJO类对象）
        DataSet<EmployeePOJO> employeeSource = env.readCsvFile("/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/employee.csv")
                .pojoType(EmployeePOJO.class,"deptId","name","salary");

        // 读取csv数据（方式一：映射POJO类对象）
        DataSet<EmployeePOJO> employee2Source = env.readCsvFile("/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/employee2.csv")
                .pojoType(EmployeePOJO.class,"deptId","name","salary");

        // 读取csv数据（方式一：映射POJO类对象）
        DataSet<DeptPOJO> deptSource = env.readCsvFile("/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/dept.csv")
                .pojoType(DeptPOJO.class,"deptId","name");


        /**
         * union() -> map() -> groupBy() -> reduce()
         */
        DataSet<DeptSalaryPOJO> unionResult = employeeSource.union(employee2Source)
                .map(new MapFunction<EmployeePOJO, DeptSalaryPOJO>() {
                    @Override
                    public DeptSalaryPOJO map(EmployeePOJO employeePOJO) throws Exception {
                        return new DeptSalaryPOJO(employeePOJO.deptId,employeePOJO.salary);
                    }
                })
                .groupBy("deptId")
                .reduce(new ReduceFunction<DeptSalaryPOJO>() {
                    @Override
                    public DeptSalaryPOJO reduce(DeptSalaryPOJO t1, DeptSalaryPOJO t2) throws Exception {
                        return new DeptSalaryPOJO(t1.deptId,t1.salary + t2.salary);
                    }
                });

        unionResult.print();

        /**
         * DeptSalaryPOJO{deptId='100', salary=70800}
         * DeptSalaryPOJO{deptId='400', salary=15600}
         * DeptSalaryPOJO{deptId='300', salary=25000}
         * DeptSalaryPOJO{deptId='200', salary=24500}
         */


        /**
         * join() -> map()
         */
        DataSet<Tuple3<String, String, Integer>> joinResult = unionResult.join(deptSource).where("deptId").equalTo("deptId")
            .map(new MapFunction<Tuple2<DeptSalaryPOJO, DeptPOJO>, Tuple3<String, String, Integer>>() {
                @Override
                public Tuple3<String, String, Integer> map(Tuple2<DeptSalaryPOJO, DeptPOJO> tuple2) throws Exception {
                    return new Tuple3<>(tuple2.f0.deptId,tuple2.f1.name,tuple2.f0.salary);
                }
            });

        joinResult.print();

        /**
         * (100,技术部,70800)
         * (400,采购部,15600)
         * (300,营销部,25000)
         * (200,市场部,24500)
         */
    }
}
