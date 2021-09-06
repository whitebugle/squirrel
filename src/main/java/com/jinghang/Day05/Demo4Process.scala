package com.jinghang.Day05

import java.time.Duration

import com.jinghang.Day02.SensorReading
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object Demo4Process {
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
    //监控温度传感器的温度值，如果温度值在2秒钟之内(processing time)连 续上升，则报警。
    val processds: DataStream[String] = ds.keyBy(_.sensorId)
      .process(new TempIncreaseAlertFunction)
    processds.print("pro")
    env.execute("job")
  }

}

class TempIncreaseAlertFunction extends KeyedProcessFunction[String,SensorReading,String] {

  //定义状态保存上一个传感器温度值
  var lastTempState: ValueState[Double] = _
  // 保存注册的定时器的时间戳
  var timerState: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTempState", classOf[Double]) {})
    timerState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerState", classOf[Long]))
  }

  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    val lasttemp = lastTempState.value()
    lastTempState.update(i.temperature)
    val lasttime = timerState.value()
    if (i.temperature <= lasttemp || (lasttemp == 0 && lasttime == 0)) {
      //删除定时器，清空标记状态
      context.timerService().deleteEventTimeTimer(lasttime)
      timerState.clear()
    } else if (i.temperature > lasttemp && lasttime == 0) {
      val ts = i.timestamp + 2
      context.timerService().registerEventTimeTimer(ts)
      timerState.update(ts)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("传感器 id 为: " + ctx.getCurrentKey + "的传感器温度值已经连续 2s 上升了。")
    timerState.clear()
  }
}