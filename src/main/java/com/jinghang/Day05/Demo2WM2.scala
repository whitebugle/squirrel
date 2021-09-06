package com.jinghang.Day05

import java.text.SimpleDateFormat
import java.time.Duration

import com.jinghang.Day02.SensorReading
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

object Demo2WM2 {
  def main(args: Array[String]): Unit = {
    val sdf = new SimpleDateFormat("HH:mm:ss")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val mds: DataStream[SensorReading] = env.addSource(new SourceFunction[SensorReading] {
      private var flag = true

      override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
        val random = new Random()
        while (flag) {
          val sensorId = "sensor_" + random.nextInt(10)
          val timestamp = System.currentTimeMillis() - random.nextInt(5) * 1000L
          val temperature = 20 * random.nextGaussian() + 20
          println("发送的数据为：" + sensorId + temperature +"time:"+ sdf.format(timestamp))
          sourceContext.collect(SensorReading(sensorId, timestamp, temperature))
          Thread.sleep(1000)
        }
      }

      override def cancel(): Unit = flag = false
    })
    val mdswm = mds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(
      new SerializableTimestampAssigner[SensorReading] {
        override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp
      }
    ))
    val result = mdswm.keyBy(_.sensorId)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .process(new ProcessWindowFunction[SensorReading, String, String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[String]): Unit = {
          val start = sdf.format(context.window.getStart)
          val end = sdf.format(context.window.getEnd)
          val outstr: String = String.format("key:%s,窗口开始结束:[%s~%s),属于该窗口的事件时间:%s", key.toString, start, end, elements.toIterator.toBuffer)
          out.collect(outstr)
        }
      })
    result.print()
    env.execute("job")

  }

}
