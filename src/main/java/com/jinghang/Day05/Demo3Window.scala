package com.jinghang.Day05

import java.time.Duration

import com.jinghang.Day02.SensorReading
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object Demo3Window {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val fds = env.readTextFile("E:\\jinghang\\实训01\\Day12-3\\sensor.txt")

    val ds: DataStream[SensorReading] = fds.map(line => {
      val splits = line.split(",")
      SensorReading(splits(0).trim, splits(1).trim.toLong, splits(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
        override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp*1000L
      }))

    ds.map(sensor=>(sensor.sensorId,sensor.temperature))
      .keyBy(_._1)
     .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
      //.window(TumblingEventTimeWindows.of(Time.seconds(10)))
     // .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
     // .countWindow()


    env.execute("job")
  }

}
