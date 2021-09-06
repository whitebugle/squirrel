package com.jinghang.Day03;

import com.jinghang.Day02.SensorReading;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo01RichfuncJ {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<String> filedss = env.readTextFile("E:\\jinghang\\实训01\\Day12-3\\sensor.txt");

        SingleOutputStreamOperator<SensorReading> mapds = filedss.map(line -> new SensorReading(
                line.split(",")[0].trim(),
                Long.parseLong(line.split(",")[1].trim()),
                Double.parseDouble(line.split(",")[2].trim())
        ));

        SingleOutputStreamOperator<SensorReading> fitds = mapds.filter(new Myjavafilter());
        fitds.print("filter");
        env.execute("job");
    }

    static class Myjavafilter extends RichFilterFunction<SensorReading> {

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.err.println("start");
        }



        @Override
        public boolean filter(SensorReading sensorReading) throws Exception {
            return "sensor_3".equals(sensorReading.sensorId());
        }

        @Override
        public void close() throws Exception {
            System.err.println("finish");
            super.close();
        }
    }
}

