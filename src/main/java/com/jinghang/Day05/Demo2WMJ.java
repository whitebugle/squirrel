package com.jinghang.Day05;

import com.jinghang.Day02.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

public class Demo2WMJ {
    public static void main(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<SensorReading> dss = env.addSource(new SourceFunction<SensorReading>() {
            private Boolean flag = true;
            Random random = new Random();

            @Override
            public void run(SourceContext<SensorReading> sourceContext) throws Exception {
                while (flag) {
                    String sensorid = "sensor_" + random.nextInt(5);
                    Long timestamp = System.currentTimeMillis() - random.nextInt(5) * 1000;
                    Double temp = random.nextGaussian() * 20 + 20;
                    System.out.println("发送的数据为：" + sensorid + temp + "time:" + sdf.format(timestamp));
                    sourceContext.collect(new SensorReading(sensorid, timestamp, temp));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });
        SingleOutputStreamOperator<SensorReading> wmds = dss.assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                    @Override
                    public long extractTimestamp(SensorReading sensorReading, long l) {
                        return sensorReading.timestamp();
                    }
                }));
        wmds.keyBy(a->a.sensorId())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<SensorReading, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<SensorReading> iterable, Collector<String> collector) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        ArrayList<SensorReading> list = new ArrayList<>();
                        Iterator<SensorReading> iterator = iterable.iterator();
                        while (iterator.hasNext()){
                            list.add(iterator.next());
                        }
                        String out=String.format("key:%s,窗口开始结束:[%s~%s),属于该窗口的事件时间:%s", s, start, end, Arrays.toString(list.toArray()));
                        collector.collect(out);
                    }
                })
                .print("selfout");
        env.execute("job");


    }
}
