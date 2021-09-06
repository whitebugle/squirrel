package com.jinghang.Day01

import org.apache.flink.api.scala._


object Demo01batch {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val ds01: DataSet[String] = env.readTextFile("E:\\jinghang\\实训01\\Day10-1\\flink.txt")
    val res: AggregateDataSet[(String, Int)] = ds01.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    res.print()

  }

}
