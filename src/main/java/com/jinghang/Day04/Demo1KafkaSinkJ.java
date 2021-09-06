package com.jinghang.Day04;

import com.jinghang.Day02.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class Demo1KafkaSinkJ {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> filedss = env.readTextFile("E:\\jinghang\\实训01\\Day12-3\\sensor.txt");

        SingleOutputStreamOperator<String> mapds = filedss.map(line -> new SensorReading(
                line.split(",")[0].trim(),
                Long.parseLong(line.split(",")[1].trim()),
                Double.parseDouble(line.split(",")[2].trim())
        ).toString());

        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop02:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");


        mapds.addSink(new FlinkKafkaProducer011<String>(
                "test",
                new SimpleStringSchema(),
                prop
        ));
        env.execute("job");
    }
}
