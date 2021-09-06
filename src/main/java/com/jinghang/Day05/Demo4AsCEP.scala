package com.jinghang.Day05

import java.time.Duration
import java.util

import com.jinghang.Day02.SensorReading
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Demo4AsCEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val fds = env.readTextFile("E:\\jinghang\\实训01\\Day12-3\\sensor.txt")

    val ds= fds.map(line=>{
      val splits = line.split(",")
      SensorReading(splits(0).trim, splits(1).trim.toLong, splits(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
        override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp*1000L
      }))

    //sensorId: String, timestamp: Long, temperature: Double
    val pattern=Pattern.begin[SensorReading]("begin").where(_.temperature!=null)
      .next("next").where((sensor,ntx)=>{
      val last = ntx.getEventsForPattern("begin").iterator.next()
      sensor.sensorId==last.sensorId && sensor.temperature>last.temperature
  })
      .next("final").where((sensor,ntx)=>{
      val last = ntx.getEventsForPattern("next").iterator.next()
      sensor.sensorId==last.sensorId && sensor.temperature>last.temperature
    })
      .within(Time.seconds(2))

    val ps: PatternStream[SensorReading] = CEP.pattern(ds,pattern)

    ps.select(new Selectfire).print()
    env.execute("hot job")
}
}
class Selectfire extends PatternSelectFunction[SensorReading,String]{
  override def select(map: util.Map[String, util.List[SensorReading]]): String = {
    val matchSensor: SensorReading = map.get("final").get(0)
    val id = matchSensor.sensorId
    val time = matchSensor.timestamp
    val temp = matchSensor.temperature
    s"$id 于 $time 连续两次升温,最后温度为 $temp"
  }
}