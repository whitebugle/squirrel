package com.jinghang.Day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class Demo2SourceJ {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dss = env.readTextFile("E:\\jinghang\\实训01\\Day12-3\\sensor.txt");


        DataStreamSource<SensorReading> streamSource = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_2", 1547718599L, 36.655666555),
                new SensorReading("sensor_3", 1567718199L, 38.6575566555),
                new SensorReading("sensor_4", 1587486456L, 32.6545566555),
                new SensorReading("sensor_5", 1597718199L, 35.6565566555)
        ));
        SingleOutputStreamOperator<SensorReading> mapstream = dss.map(line -> new SensorReading(line.split(",")[0].trim(),
                Long.parseLong(line.split(",")[1].trim()),
                Double.parseDouble(line.split(",")[2].trim())));

        DataStreamSource<String> dss01 = env.fromElements("a", "u", "ok");
        dss01.print("element");
        env.execute("job");
    }



    static class SensorReading{
        String sensorId;
        Long timestamp;
        Double temperature;
        public SensorReading(String id,Long time,Double temp){
            this.sensorId=id;
            this.timestamp=time;
            this.temperature=temp;
        }
    }
}
