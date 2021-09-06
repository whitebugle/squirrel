package com.jinghang.Day01

import org.apache.flink.streaming.api.scala._

object Demo2stream {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

   //全局设置并行度为1
    senv.setParallelism(1)

    //禁止job chain
    //env.disableOperatorChaining()

    val ds1: DataStream[String] = senv.socketTextStream("hadoop02",9999)
    ds1.flatMap(_.split(" "))
      .map((_,1)).setParallelism(1)//单独设置api并行度
      .keyBy(0)
      .sum(1)
      .print()
    senv.execute("WCS job")

  }

}
