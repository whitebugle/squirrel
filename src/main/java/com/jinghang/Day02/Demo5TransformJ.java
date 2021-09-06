package com.jinghang.Day02;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo5TransformJ {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<String> dss = env.readTextFile("E:\\jinghang\\实训01\\Day12-3\\sensor.txt");
        SingleOutputStreamOperator<SensorReading> singleOutputStreamOperator = dss.map(line -> new SensorReading(line.split(",")[0].trim(),
                Long.parseLong(line.split(",")[1].trim()),
                Double.parseDouble(line.split(",")[2].trim())));
        singleOutputStreamOperator.filter(a->a.sensorId()=="sensor_3").print("filter");
        KeyedStream<SensorReading, String> keystream = singleOutputStreamOperator.keyBy(a -> a.sensorId());
        SingleOutputStreamOperator<SensorReading> keybystream = keystream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading sr1, SensorReading sr2) throws Exception {
                return new SensorReading(sr1.sensorId(), sr1.timestamp(), sr1.temperature() < sr2.temperature() ? sr1.temperature() : sr2.temperature());
            }
        });

        keybystream.print("key");
        env.execute("job");
    }
}
