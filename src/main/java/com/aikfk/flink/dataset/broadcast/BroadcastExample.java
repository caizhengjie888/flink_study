package com.aikfk.flink.dataset.broadcast;

import com.aikfk.flink.base.EmployeePOJO;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.List;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/10 2:04 下午
 */
public class BroadcastExample {
    public static void main(String[] args) throws Exception {

        // 准备环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取csv数据（方式一：映射POJO类对象）
        DataSet<EmployeePOJO> dataSource = env.readCsvFile("/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/employee.csv")
                .pojoType(EmployeePOJO.class,"deptId","name","salary");

        DataSet<EmployeePOJO> broadcastDataSet = env.fromElements(new EmployeePOJO("100","alex",12000));

        /**
         * EmployeePOJO -> filter() -> 过滤出广播变量
         */
        DataSet<EmployeePOJO> filterSource =  dataSource.filter(new RichFilterFunction<EmployeePOJO>() {

            List<EmployeePOJO> broadList = null;
            @Override
            public boolean filter(EmployeePOJO employeePOJO) throws Exception {

                for (EmployeePOJO broad : broadList){
                    return ! employeePOJO.name.equals(broad.name);
                }
                return false;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                // 对广播变量进行访问
                broadList = getRuntimeContext().getBroadcastVariable("broadcastDataSet");
            }
            // 注册广播变量
        }).withBroadcastSet(broadcastDataSet,"broadcastDataSet");

        filterSource.print();
    }
}
