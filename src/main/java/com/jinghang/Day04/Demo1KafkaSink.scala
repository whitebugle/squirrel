package com.jinghang.Day04

import java.util.Properties

import com.jinghang.Day02.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.kafka.clients.producer.ProducerConfig

object Demo1KafkaSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val dsf: DataStream[String] = env.readTextFile("E:\\jinghang\\实训01\\Day12-3\\sensor.txt")
    val ds01 = dsf.map(line => {
      val splits = line.split(",")
      SensorReading(splits(0).trim, splits(1).trim.toLong, splits(2).trim.toDouble).toString
    })
    val prop = new Properties()
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop02:9092")
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

    ds01.addSink(new FlinkKafkaProducer011[String](
      "test",
      new SimpleStringSchema(),
      prop
    ))
    env.execute("job")
  }

}
