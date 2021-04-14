package com.aikfk.flink.dataset.transform

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector

object CombineGroupScala {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment;
    val dataSource = env.fromElements(
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
      .groupBy(0)
      .combineGroup((iterator,combine_collector : Collector[(String,Int)]) => {
        combine_collector.collect(iterator reduce((t1,t2) => (t1._1,t1._2 + t2._2)))
      })
      .groupBy(0)
      .reduceGroup((x => x reduce((x,y) => (x._1,x._2 + y._2))))
      .print()

  }
}
