package com.jinghang.flink12.TableTest

import java.time.Duration

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.{CheckpointConfig, ExecutionCheckpointingOptions}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object Demo4Mysql {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()

    val tenv = StreamTableEnvironment.create(env,settings)

    val tableConfig = tenv.getConfig.getConfiguration
    // 开启checkpoint
    tableConfig.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
    // checkpoint的超时时间周期，1 分钟做一次checkpoint, 每次checkpoint 完成后 sink 才会执行
    tableConfig.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(60))
    // checkpoint的超时时间, 检查点一分钟内没有完成将被丢弃
    tableConfig.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofSeconds(60))
    // checkpoint 最小间隔，两个检查点之间至少间隔 30 秒
    tableConfig.set(ExecutionCheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, Duration.ofSeconds(30))
    // 同一时间只允许进行一个检查点
    tableConfig.set(ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, Integer.valueOf(1))
    // 手动cancel时是否保留checkpoint
    tableConfig.set(ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT, CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val flink_source_table=
      """
        |CREATE TABLE mysqlsource (
        |sensor STRING,
        |temps DOUBLE
        |)
        |WITH(
        |'connector' = 'jdbc',
        |'url' = 'jdbc:mysql://localhost:3306/flink?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
        |'username' = 'root',
        |'password' = 'wu555555',
        |'table-name' = 'temperatures',
        |'driver' = 'com.mysql.cj.jdbc.Driver',
        |'scan.fetch-size' = '200'
        |)
      """.stripMargin

    val flink_sink_table=
      """
        |CREATE TABLE mysqlsink (
        |sensor STRING,
        |temps DOUBLE
        |)
        |WITH(
        |'connector' = 'print'
        |)
      """.stripMargin

  tenv.executeSql(flink_source_table)
    tenv.executeSql(flink_sink_table)
    val insertsql="insert into mysqlsink select * from mysqlsource"
    tenv.executeSql(insertsql)
    /*env.execute("job")*/ //No operators defined in streaming topology. Cannot execute
  }
}
