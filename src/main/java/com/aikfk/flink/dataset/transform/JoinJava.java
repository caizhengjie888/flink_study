package com.aikfk.flink.dataset.transform;

import com.aikfk.flink.base.EmployeePOJO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/8 3:13 下午
 */
public class JoinJava {
    public static void main(String[] args) throws Exception {

        // 准备环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取csv数据（方式二：映射成Tuple类，带有两个个字段）
        DataSet<Tuple2<String,String>> deptSource = env.readCsvFile("/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/dept.csv")
                .types(String.class,String.class);

        deptSource.print();

        /**
         * (200,市场部)
         * (100,技术部)
         * (400,采购部)
         * (300,营销部)
         */

        // 读取csv数据（方式一：映射POJO类对象）
        DataSet<EmployeePOJO> employeeSource = env.readCsvFile("/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/employee.csv")
                .pojoType(EmployeePOJO.class,"deptId","name","salary");

        employeeSource.print();

        /**
         * EmployeePOJO{deptId='100', name='alex', salary=15000}
         * EmployeePOJO{deptId='200', name='jone', salary=6700}
         * EmployeePOJO{deptId='100', name='lili', salary=8000}
         * EmployeePOJO{deptId='400', name='lucy', salary=7800}
         * EmployeePOJO{deptId='300', name='cherry', salary=12000}
         * EmployeePOJO{deptId='200', name='tony', salary=5000}
         * EmployeePOJO{deptId='100', name='jack', salary=34000}
         */

        /**
         * join() -> map()
         */
        DataSet<Tuple3<String,String,String>> joinResult = deptSource.join(employeeSource).where(0).equalTo("deptId")
                .map(new MapFunction<Tuple2<Tuple2<String, String>, EmployeePOJO>, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> map(Tuple2<Tuple2<String, String>, EmployeePOJO> tuple2) throws Exception {
                        return new Tuple3<>(tuple2.f0.f0,tuple2.f0.f1,tuple2.f1.name);
                    }
                });

        joinResult.print();

        /**
         * (100,技术部,jack)
         * (100,技术部,alex)
         * (400,采购部,lucy)
         * (100,技术部,lili)
         * (300,营销部,cherry)
         * (200,市场部,jone)
         * (200,市场部,tony)
         */
    }
}
