package com.jinghang.Day03

import com.jinghang.Day02.SensorReading
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object Demo01Richfunc {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.disableOperatorChaining()
    val dsf: DataStream[String] = env.readTextFile("E:\\jinghang\\实训01\\Day12-3\\sensor.txt")
    val ds01 = dsf.map(line => {
      val splits = line.split(",")
      SensorReading(splits(0).trim, splits(1).trim.toLong, splits(2).trim.toDouble)
    })
    val fitds: DataStream[SensorReading] = ds01.filter(new Myfilter)
    fitds.print("filter")
    env.execute("job")
  }
}
class Myfilter extends RichFilterFunction[SensorReading]{
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    println("start")
  }

  override def filter(t: SensorReading): Boolean = {
      t.sensorId=="sensor_3"
  }



  override def close(): Unit = {
    println("finish")
    super.close()
  }
}
