package com.jinghang.flink12.TableTest

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}

object Demo2FlinkTable {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val ds: DataStream[String] = env.readTextFile("E:\\jinghang\\实训01\\Day12-3\\sensor.txt")
    val ds1 = ds.map(line => {
      val splits = line.split(",")
      SensorReading(splits(0).trim, splits(1).trim.toLong, splits(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
          override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp * 1000
        }))
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tenv = StreamTableEnvironment.create(env,settings)
    val dstable: Table = tenv.fromDataStream(ds1)
    dstable.printSchema()

    val selecttable: Table = dstable.select("sensorId,timestamp,temperature").filter("sensorId='sensor_3'")

    tenv.toAppendStream[(String,Long,Double)](selecttable).print()

    tenv.createTemporaryView("tableDs",dstable)

    var sql ="select * from tableDs where sensorId = 'sensor_3'"
    val restable = tenv.sqlQuery(sql)
    val resds: DataStream[(String, Long, Double)] = tenv.toAppendStream[(String,Long,Double)](restable)
    resds.print()
    //可对table对象进行查询
    sql =s"select sensorId,count(1) from $dstable group by sensorId"
    val globaltable = tenv.sqlQuery(sql)
    tenv.toRetractStream[(String,Long)](globaltable).filter(_._1).print()
    env.execute("job")
  }
}
