package com.jinghang.Day04;

import com.jinghang.Day02.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

import java.time.Duration;

public class Demo3WindowJ {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dss = env.readTextFile("E:\\jinghang\\实训01\\Day12-3\\sensor.txt");
        SingleOutputStreamOperator<SensorReading> mapds = dss.map(line -> new SensorReading(
                line.split(",")[0].trim(),
                Long.parseLong(line.split(",")[1].trim()),
                Double.parseDouble(line.split(",")[2].trim())
        )).assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
            .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                @Override
                public long extractTimestamp(SensorReading sensorReading, long l) {
                    return sensorReading.timestamp()*1000;
                }
            })
        );
        mapds.map(new MapFunction<SensorReading, Tuple2<String,Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<String, Double>(sensorReading.sensorId(),sensorReading.temperature());
            }
        })
                .keyBy(a->a._1)
                .reduce((a,b)->new Tuple2(a._1,Math.min( a._2, b._2)))
                .print("min");
        env.execute("job");


    }
}
