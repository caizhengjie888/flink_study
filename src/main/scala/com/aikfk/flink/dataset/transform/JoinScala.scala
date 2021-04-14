package com.aikfk.flink.dataset.transform

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object JoinScala {
  case class employee(deptId:String,name:String,salary:Int)
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment;

    // 读取csv数据（方式二：映射成Tuple类，带有两个个字段）
    val deptSource = env.readCsvFile[(String,String)]("/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/dept.csv")

    // 读取csv数据（方式一：映射POJO类对象）
    val employeeSource = env.readCsvFile[employee]("/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/employee.csv")

    // join -> map
    val joinResult = deptSource.join(employeeSource).where(0).equalTo("deptId")
      .map(tuple2 => (tuple2._1._1,tuple2._1._2,tuple2._2.name))

    joinResult.print()

    /**
     * (400,采购部,lucy)
     * (100,技术部,jack)
     * (100,技术部,alex)
     * (100,技术部,lili)
     * (300,营销部,cherry)
     * (200,市场部,tony)
     * (200,市场部,jone)
     */
  }
}
