package com.jinghang.Day02

import org.apache.flink.streaming.api.scala._

object Demo06Split {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
      val ds = env.readTextFile("E:\\jinghang\\实训01\\Day12-3\\sensor.txt")
    val ds01: DataStream[SensorReading] = ds.map(line => {
      val splits = line.split(",")
      SensorReading(splits(0), splits(1).trim.toLong, splits(2).trim.toDouble)
    })
    val sps: SplitStream[SensorReading] = ds01.split(a => {
      if (a.temperature > 30) {
        Seq("high")//标识
      } else {
        Seq("low")
      }
    })
    val highstream: DataStream[SensorReading] = sps.select("high")
      val lowstream = sps.select("low")
    val allstream: DataStream[SensorReading] = sps.select("high","low")
    highstream.print("high")
    allstream.print("all")

    val unds: DataStream[SensorReading] = highstream.union(lowstream)
    val conds: ConnectedStreams[SensorReading, SensorReading] = highstream.connect(allstream)
    env.execute("job")
  }

}
