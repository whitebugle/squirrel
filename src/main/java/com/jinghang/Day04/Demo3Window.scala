package com.jinghang.Day04



import java.time.Duration

import com.jinghang.Day02.SensorReading
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Demo3Window {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds: DataStream[String] = env.readTextFile("E:\\jinghang\\实训01\\Day12-3\\sensor.txt")
    val ds01 = ds.map(line => {
      val splits = line.split(",")
      SensorReading(splits(0).trim, splits(1).trim.toLong, splits(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
          override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp * 1000L
        })
      )

    ds01.map(sensor=>{
      (sensor.sensorId,sensor.temperature)
    })
      .keyBy(_._1)
      //.timeWindow(Time.seconds(5),Time.seconds(5))
      //.timeWindow(Time.seconds(5))
      .countWindow(10,10)
      //.countWindow(5)
      .reduce((s1,s2)=>(s1._1,s1._2.min(s2._2)))
      .print("window")
    env.execute("job")
  }

}
