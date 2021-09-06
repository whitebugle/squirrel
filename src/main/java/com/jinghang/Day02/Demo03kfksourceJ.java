package com.jinghang.Day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class Demo03kfksourceJ {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop02:9092");
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"consumer");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        DataStreamSource<String> kfkdss = env.addSource(new FlinkKafkaConsumer011<String>(
                "test", new SimpleStringSchema(), prop));
        kfkdss.print("kfkdss");
        env.execute("job");
    }
}
