package com.jinghang.Day02

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object Demo4UDSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val Mds: DataStream[SensorReading] = env.addSource(new MySource)

    Mds.print("Mysource")
    env.execute("job")

  }
}

class MySource extends SourceFunction[SensorReading]{
  var running=true
  var mark=0

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    while (running){
      val id="Sensor_"+Random.nextInt(6)
      val time=System.currentTimeMillis()
      val temperature=Random.nextGaussian()*20+40.0
      sourceContext.collect(SensorReading(id,time,temperature))
      mark+=1
      if (mark>10){
        cancel()
      }
    }
  }

  override def cancel(): Unit = running=false
}
