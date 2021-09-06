package com.jinghang.flink12.TableTest

import java.time.Duration

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog




object Demo01Table {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)

    val ds: DataStream[SensorReading] = env.fromElements(
      SensorReading("sensor_4", 1547718202, 33.80018327300259),
      SensorReading( "sensor_5", 1547718203, 38.80018327300259),
      SensorReading( "sensor_6", 1547718204, 45.80018327300259),
      SensorReading( "sensor_7", 1547718205, 39.80018327300259),
      SensorReading( "sensor_3", 1547718210, 44.80018327300259),
      SensorReading( "sensor_3", 1547718211, 100.8001832730025),
      SensorReading( "sensor_3", 1547718221, 10.80018327300259),
      SensorReading( "sensor_3", 1547718250, 20.80018327300259)
    )
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3))
        .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
          override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp*1000
        }))

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()

    val tenv = StreamTableEnvironment.create(env,settings)

    val table: Table = tenv.fromDataStream(ds)

    new HiveCatalog()

    table.printSchema()
    /*
   root
|-- sensorId: String
|-- timestamp: Long
|-- temperature: Double
    */

    val table1: Table = table.select("sensorId,timestamp,temperature")
    val value: DataStream[(String, Long, Double)] = tenv.toAppendStream[(String,Long,Double)](table1)

    /*
   toAppendStream和toRetractStream的区别:
   toAppendStream使用场景是追加模式，只有在动态Table仅通过INSERT更改修改时才能使用此模式，
   即它仅附加，并且以前发出的结果永远不会更新。如果更新或删除操作使用追加模式会失败报错；
   toRetractStream缩进模式： 始终可以使用此模式。返回值是boolean类型。
   它用true或false来标记数据的插入和撤回，返回true代表数据插入，false代表数据的撤回。
 */
    //会导致原有数据或展示的已有数据更改的操作用toRetractStream

    tenv.createTemporaryView("sensor",ds)
    var sql = "select * from sensor where sensorId = 'sensor_3'"
    val table2 = tenv.sqlQuery(sql)
    val table2ds: DataStream[(String, Long, Double)] = tenv.toAppendStream[(String,Long,Double)](table2)
    val sql1="select sensorId,count(1) from sensor group by sensorId"
    val table3 = tenv.sqlQuery(sql1)
    tenv.toRetractStream[(String,Long)](table3).print()
    env.execute("job")
  }
}

case class SensorReading(sensorId:String,timestamp:Long,temperature:Double)