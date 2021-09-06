package com.jinghang.Day02

import org.apache.flink.streaming.api.scala._

object Demo5Transform {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds: DataStream[String] = env.readTextFile("E:\\jinghang\\实训01\\Day12-3\\sensor.txt")
    val ds01: DataStream[SensorReading] = ds.map(line => {
      val splits = line.split(",")
      SensorReading(splits(0).trim, splits(1).trim.toLong, splits(2).trim.toDouble)
    })
    val ds02: DataStream[String] = env.fromElements("are u ok").flatMap(_.split(" "))
    val ds03: DataStream[Array[String]] = env.fromElements("are u ok").map(_.split(" "))
    ds01.filter(_.sensorId=="sensor_3").print("filter")
    val keyds: KeyedStream[SensorReading, String] = ds01.keyBy(_.sensorId)
    keyds.reduce((a,b)=>{
      SensorReading(a.sensorId,a.timestamp,a.temperature.min(b.temperature))
    }).print("min")

    keyds.reduce((a,b)=>{
      var time=0l
      if (a.temperature>b.temperature) {
        time=b.timestamp
      }else{
        time=a.timestamp
      }
      SensorReading(a.sensorId,time,a.temperature.min(b.temperature))
    }).print("mymin")

    env.execute("job")
  }
}
