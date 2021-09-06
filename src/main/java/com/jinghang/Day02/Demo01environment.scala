package com.jinghang.Day02

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Demo01environment {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val lenv = StreamExecutionEnvironment.createLocalEnvironment()

    val renv = StreamExecutionEnvironment.createRemoteEnvironment("",99999)

    env.setParallelism(1)        //设置全局并行度

    env.disableOperatorChaining()//禁用job chain
  }

}
