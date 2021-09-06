package com.jinghang.Day05

import java.time.Duration

import com.jinghang.Day02.SensorReading
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object Demo5SideOutPut {

  //侧输出流对象
  val outPutTag: OutputTag[SensorReading] = new OutputTag[SensorReading]("output")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
   val ds= env.readTextFile("E:\\jinghang\\实训01\\Day12-3\\sensor.txt")
      .map(line=>{
        val splits = line.split(",")
        SensorReading(splits(0).trim,splits(1).trim.toLong,splits(2).trim.toDouble)
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
        override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp*1000L
      }))

    ds.process(new FreezingMonitor)
        .getSideOutput(outPutTag)
        .print("sideout")
    env.execute("job")
  }

  class FreezingMonitor extends ProcessFunction[SensorReading,SensorReading]{

    override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
      if (i.temperature>32){
        context.output(outPutTag,i)
      }else{
        collector.collect(i)
      }
    }

  }
}

