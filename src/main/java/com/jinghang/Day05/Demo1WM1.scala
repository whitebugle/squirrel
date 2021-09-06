package com.jinghang.Day05


import java.time.Duration

import com.jinghang.Day02.SensorReading
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPunctuatedWatermarksAdapter


object Demo1WM1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val fds = env.readTextFile("E:\\jinghang\\实训01\\Day12-3\\sensor.txt")

    val ds: DataStream[SensorReading] = fds.map(line => {
      val splits = line.split(",")
      SensorReading(splits(0).trim, splits(1).trim.toLong, splits(2).trim.toDouble)
    })
       /* .assignAscendingTimestamps(_.timestamp*1000)*/ //时间有序数据


  /*      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {
      override def extractTimestamp(t: SensorReading): Long = t.timestamp*1000L
    })*/   //过时方法

        /*.assignTimestampsAndWatermarks(new Assign01)*/
        /*.assignTimestampsAndWatermarks(new Assign02)*/

      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
          override def extractTimestamp(t: SensorReading, l: Long): Long = {
            t.timestamp * 1000L
          }
        }))
    ds.print("")
    env.execute("job")

  }

}

//周期生成
class Assign01() extends AssignerWithPeriodicWatermarks[SensorReading]{
  var maxEventTime=Long.MinValue
  val bound=2*1000

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxEventTime-bound)
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    val eventtime = t.timestamp*1000
    maxEventTime=eventtime.max(eventtime)
    eventtime
  }
}

//条件生成
class Assign02 extends AssignerWithPunctuatedWatermarks[SensorReading]{
  var  maxEventTime=Long.MinValue
  val  bound=2*1000

  override def checkAndGetNextWatermark(t: SensorReading, l: Long): Watermark = {
    if ("sensor_3" == t.sensorId){
      new Watermark(maxEventTime-bound)
    }else{
      null
    }
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    val eventTime = t.timestamp*1000L
    maxEventTime=maxEventTime.max(eventTime)
    eventTime
  }
}
