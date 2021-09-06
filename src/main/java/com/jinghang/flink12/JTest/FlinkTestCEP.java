package com.jinghang.flink12.JTest;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class FlinkTestCEP {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> fds = env.readTextFile("/");

        SingleOutputStreamOperator<Loginlog> ds = fds.map(new MapFunction<String, Loginlog>() {
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

        Pattern<Loginlog, Loginlog> pattern = Pattern.<Loginlog>begin("begin").where(new SimpleCondition<Loginlog>() {
            @Override
            public boolean filter(Loginlog loginlog) throws Exception {
                return loginlog.time > (System.currentTimeMillis() / 1000 / 60 / 60 - 6) * 1000 * 60 * 60;
            }
        }).followedBy("follow").where(new IterativeCondition<Loginlog>() {
            @Override
            public boolean filter(Loginlog loginlog, Context<Loginlog> context) throws Exception {

               /*     val begin: Iterable[LoginEvent] = cxt.getEventsForPattern("begin")
                    lg.userId == begin.iterator.next().userId && lg.eventType == "fail"*/
                Iterable<Loginlog> begin = context.getEventsForPattern("begin");
                return begin.iterator().next().id == loginlog.id && loginlog.time < System.currentTimeMillis();
            }
        });

        CEP.pattern(ds,pattern).select(new PatternSelectFunction<Loginlog, String>() {
            @Override
            public String select(Map<String, List<Loginlog>> map) throws Exception {
                Loginlog begin = map.get("begin").get(0);
                Loginlog follow = map.get("follow").get(0);

                return begin.getId()+"在"+begin.getTime()+"登录，之后在"+follow.getTime()+"登录";
            }
        }).print("六小时内：");

        try {
            env.execute("loginjob");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
