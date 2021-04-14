package com.aikfk.flink.dataset.transform

import org.apache.flink.api.scala.{ExecutionEnvironment,_}

object MapScala {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment;

    val dataSource = env.fromElements(
      "java java spark hive",
      "hive java java spark",
      "java java hadoop"
    ).map(line => line.toUpperCase())
      .print()
  }
}
