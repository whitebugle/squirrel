package com.jinghang.Day02

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig

object Demo3Kfksource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)

    val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop02:9092")
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"consumer")
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")
    val kfkds: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String](
      "test",
      new SimpleStringSchema(),
      prop
    ))
    kfkds.print("kafka source")

    env.execute("job")
  }

}
