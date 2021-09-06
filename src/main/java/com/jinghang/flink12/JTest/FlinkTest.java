package com.jinghang.flink12.JTest;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;
import java.util.*;

public class FlinkTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        Properties prop = new Properties();

        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"consumer");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<String>("Test1", new SimpleStringSchema(), prop));
        SingleOutputStreamOperator<Loginlog> mapds = dataStreamSource.map(new MapFunction<String, Loginlog>() {
            @Override
            public Loginlog map(String s) throws Exception {
                String[] split = s.split(" ");

                return new Loginlog(split[0], split[1], split[2], Long.parseLong(split[3]));
            }
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Loginlog>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Loginlog>() {
                            @Override
                            public long extractTimestamp(Loginlog loginlog, long l) {
                                return loginlog.time * 1000L;
                            }
                        }));
        mapds.timeWindowAll(Time.hours(1))
                .apply(new AllWindowFunction<Loginlog, Map, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Loginlog> iterable, Collector<Map> collector) throws Exception {
                        Set<String> set = new HashSet<>();
                        Iterator<Loginlog> iterator = iterable.iterator();
                        while (iterator.hasNext()) {
                            String id = iterator.next().id;
                           set.add(id);
                        }
                        Map map=new HashMap();
                        map.put(timeWindow.getEnd(),set.size());
                        collector.collect(map);
                    }
                }).print("now:");

        try {
            env.execute("过去一分钟用户量");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
