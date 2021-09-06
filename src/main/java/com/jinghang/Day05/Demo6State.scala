package com.jinghang.Day05

import java.time.Duration

import com.jinghang.Day02.SensorReading
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object Demo6State {
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

    ds.keyBy(_.sensorId)
      //.process(new TemperatureAlertFunction(10))
      .flatMap(new TempAlertflatmap(10))
      .print("flatmap")
    env.execute("job")
  }

}

class TemperatureAlertFunction(threshold :Int ) extends KeyedProcessFunction[String,SensorReading,(String,Double,Double)]{
  var lastTemp:ValueState[Double]=_

  override def open(parameters: Configuration): Unit = {
    lastTemp=getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))
  }
  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, collector: Collector[(String, Double, Double)]): Unit = {
    val lastTep = lastTemp.value()
    lastTemp.update(i.temperature)
    if((i.temperature-lastTep.abs>threshold &&  lastTep!=0)){
      collector.collect(i.sensorId,lastTep,i.temperature)
    }
  }
}

class TempAlertflatmap(threshold:Int) extends RichFlatMapFunction[SensorReading,(String,Double,Double)]{

 var lastTemp: ValueState[Double]=_

  override def open(parameters: Configuration): Unit = {
    lastTemp=getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))
  }

  override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    val last = lastTemp.value()
    lastTemp.update(in.temperature)
    if ((last-in.temperature).abs>threshold && last!=0){
      collector.collect(in.sensorId,last,in.temperature)
    }
  }
}
