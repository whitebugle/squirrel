package com.jinghang.Day02;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class Demo06SplitJ {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dss = env.readTextFile("E:\\jinghang\\实训01\\Day12-3\\sensor.txt");

        SingleOutputStreamOperator<SensorReading> singleOutputStreamOperator = dss.map(line ->
                new SensorReading(line.split(",")[0].trim(),
                Long.parseLong(line.split(",")[1].trim()),
                Double.parseDouble(line.split(",")[2].trim())));

        SplitStream<SensorReading> splitStream = singleOutputStreamOperator.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                if (sensorReading.temperature() > 30) {
                    return Arrays.asList("high");
                } else {
                    return Arrays.asList("low");
                }
            }
        });
        DataStream<SensorReading> high = splitStream.select("high");
        high.print("high");
        DataStream<SensorReading> low = splitStream.select("low");
        ConnectedStreams<SensorReading, SensorReading> connect = high.connect(low);
        DataStream<SensorReading> unionds = high.union(low);
       // unionds.print("union");
        env.execute("job");
    }
}
