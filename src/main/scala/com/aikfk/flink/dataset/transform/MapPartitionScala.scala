package com.aikfk.flink.dataset.transform

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector

object MapPartitionScala {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment;

    val dataSource = env.fromElements(
      "java java spark hive",
      "hive java java spark",
      "java java hadoop"
    )
      .mapPartition((iterator:Iterator[String] , collector : Collector[String]) => {
        for (line <- iterator){
          collector.collect(line.toUpperCase)
        }
      })
      .print()
  }

}
