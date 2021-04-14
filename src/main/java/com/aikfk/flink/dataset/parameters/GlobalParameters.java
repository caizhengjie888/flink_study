package com.aikfk.flink.dataset.parameters;

import com.aikfk.flink.base.EmployeePOJO;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/10 2:04 下午
 */
public class GlobalParameters {
    public static void main(String[] args) throws Exception {

        // 准备环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取csv数据（方式一：映射POJO类对象）
        DataSet<EmployeePOJO> dataSource = env.readCsvFile("/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/employee.csv")
                .pojoType(EmployeePOJO.class,"deptId","name","salary");

        // 设置全局参数
        Configuration config = new Configuration();
        config.setInteger("limit", 10000);
        env.getConfig().setGlobalJobParameters(config);

        /**
         * EmployeePOJO -> filter()
         */
        DataSet<EmployeePOJO> filterSource =  dataSource.filter(new RichFilterFunction<EmployeePOJO>() {

            public int limit;

            @Override
            public boolean filter(EmployeePOJO employeePOJO) throws Exception {
                // 过滤出比参数1000大的值
                return employeePOJO.salary > limit;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                // 传入全局参数
                ExecutionConfig.GlobalJobParameters globalParams = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                Configuration globConf = (Configuration) globalParams;
                limit = globConf.getInteger("limit", 0);

            }
        });

        filterSource.print();
    }
}
