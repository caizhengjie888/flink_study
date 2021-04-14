package com.aikfk.flink.dataset.transform

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object UnionScala {

  case class employee(deptId:String,name:String,salary:Int)
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment;

    // 读取csv数据（方式一：映射POJO类对象）
    val employeeSource = env.readCsvFile[employee]("/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/employee.csv")

    // 读取csv数据（方式一：映射POJO类对象）
    val employeeSource2 = env.readCsvFile[employee]("/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/employee2.csv")

    // 读取csv数据（方式二：映射成Tuple类，带有两个个字段）
    val deptSource = env.readCsvFile[(String,String)]("/Users/caizhengjie/IdeaProjects/aikfk_flink/src/main/java/resources/dept.csv")


    /**
     * union() -> map() -> groupBy() -> reduce()
     */
    val unionResult = employeeSource.union(employeeSource2)
      .map(emp => (emp.deptId,emp.salary))
      .groupBy(0)
      .reduce((x ,y) => (x._1,x._2 + y._2))

    unionResult.print()

    /**
     * (100,70800)
     * (400,15600)
     * (300,25000)
     * (200,24500)
     */

    /**
     * join() -> map()
     */
    val joinResult = unionResult.join(deptSource).where(0).equalTo(0)
      .map(tuple => (tuple._1._1,tuple._2._2,tuple._1._2) )

    joinResult.print()

    /**
     * (400,采购部,15600)
     * (100,技术部,70800)
     * (300,营销部,25000)
     * (200,市场部,24500)
     */

  }
}
