package com.jinghang.Day02

import org.apache.flink.streaming.api.scala._



case class SensorReading(sensorId: String, timestamp: Long, temperature: Double)

object Demo02Source {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val list=List(SensorReading("sensor_1",1547718199,34.65566555),
      SensorReading("sensor_2",1547718599,36.655666555),
      SensorReading("sensor_3",1567718199,38.6575566555),
      SensorReading("sensor_4",1587486456,32.6545566555),
      SensorReading("sensor_5",1597718199,35.6565566555)
    )

    val ds01: DataStream[SensorReading] = env.fromCollection(list)

    val ds02: DataStream[String] = env.readTextFile("E:\\jinghang\\实训01\\Day12-3\\sensor.txt")

    val ds03 = ds02.map(line => {
      val splits: Array[String] = line.split(",")
      SensorReading(splits(0).trim, splits(1).trim.toLong, splits(2).trim.toDouble)
    })
    val ds04: DataStream[String] = env.fromElements("nihao","1","546L")

    ds03.print()
    env.execute("job")
  }

}
