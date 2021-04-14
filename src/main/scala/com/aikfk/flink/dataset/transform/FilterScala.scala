package com.aikfk.flink.dataset.transform

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector


object FilterScala {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment;
    val dataSource: Unit = env.fromElements(
      "java java spark hive",
      "hive java java spark",
      "java java hadoop"
    ).map(line => line.toUpperCase)
      .flatMap((line : String,collector : Collector[(String,Int)]) => {
//        for (word <- line.split(" ")){
//          collector.collect(word,1)
//        }
        (line.split(" ").foreach(word => collector.collect(word,1)))
      })
      .filter(tuple2 => (tuple2._1.equals("SPARK")))
      .print()

  }
}
