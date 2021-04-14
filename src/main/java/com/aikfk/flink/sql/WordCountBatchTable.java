package com.aikfk.flink.sql;

import com.aikfk.flink.sql.pojo.WC;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/4/5 10:32 下午
 */
public class WordCountBatchTable {
    public static void main(String[] args) throws Exception {
        // 1.准备环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.创建TableEnvironment(old planner)
        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.create(env);

        // 3.batch数据源
        DataSet<WC> dataSet = env.fromElements(
                new WC("java", 1),
                new WC("spark", 1),
                new WC("hive", 1),
                new WC("hbase", 1),
                new WC("hbase", 1),
                new WC("hadoop", 1),
                new WC("java", 1));

        // 4.将DataSet转换成table表
        Table table = tableEnvironment.fromDataSet(dataSet);

        // 5.通过Table API对table表做逻辑处理，生成结果表
        Table tableResult = table.groupBy($("wordName"))
                .select($("wordName"), $("freq").sum().as("freq"))
                .filter($("freq").isEqual(2));

        tableResult.printSchema();

        /**
         * root
         *  |-- wordName: STRING
         *  |-- freq: BIGINT
         */

        // 6.将table表转换为DataSet
        DataSet<WC> wcDataSet = tableEnvironment.toDataSet(tableResult, WC.class);
        wcDataSet.print();

        /**
         * WC{wordName='java', freq=2}
         * WC{wordName='hbase', freq=2}
         */

    }
}
