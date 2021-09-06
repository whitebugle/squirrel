package com.jinghang.flink12.TableTest

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableResult}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object Demo3Kafka {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()

    val tenv = StreamTableEnvironment.create(env,settings)

    val inputTable: TableResult = tenv.executeSql(
      """
| CREATE TABLE input_kafka (
|`SensorId`  STRING,
|`Timestamp` BIGINT,
|`Temperature` DOUBLE
|) WITH(
|'connector' = 'kafka',
|'topic' = 'test',
|'properties.bootstrap.servers' = 'hadoop02:9092',
|'properties.group.id' = 'testGroup',
|'scan.startup.mode' = 'latest-offset',
|'format'='json'
)
      """.stripMargin)
    val sql = "select * from input_kafka where SensorId = 'sensor_3' "
    val kfktable: Table = tenv.sqlQuery(sql)
    val kfkds: DataStream[(String, Long, Double)] = tenv.toAppendStream[(String,Long,Double)](kfktable)
    kfkds.print()

    tenv.executeSql(
      """
        |CREATE TABLE output_kafka (
        |`SensorId`  STRING,
        |`Timestamp` BIGINT,
        |`Temperature` DOUBLE
        |) WITH(
        |'connector' = 'kafka',
        |'topic'='test2',
        |'properties.bootstrap.servers' = 'hadoop02:9092',
        |'format' = 'json',
        |'sink.partitioner' = 'round-robin'
        |)
      """.stripMargin)
    tenv.executeSql(s"insert into output_kafka select * from $kfktable")
    env.execute("job")
  }
}
