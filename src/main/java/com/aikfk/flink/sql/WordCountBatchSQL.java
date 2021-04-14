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
public class WordCountBatchSQL {
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

        // Table table = tableEnvironment.fromDataSet(dataSet);

        // 4.创建虚拟表
        // tableEnvironment.createTemporaryView("wordcount" ,table);

        // 将dataSet转换为视图
        tableEnvironment.createTemporaryView("wordcount" ,dataSet,$("wordName"), $("freq"));

        // 5.通过SQL对表的查询，生成结果表
        Table tableResult = tableEnvironment.sqlQuery("select wordName,sum(freq) as freq " +
                "from wordcount group by wordName having sum(freq) > 1" );

        // 6.将table表转换为DataSet
        DataSet<WC> wcDataSet = tableEnvironment.toDataSet(tableResult, WC.class);
        wcDataSet.print();

        /**
         * WC{wordName='java', freq=2}
         * WC{wordName='hbase', freq=2}
         */
    }
}
