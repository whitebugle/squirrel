package com.jinghang.Day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;
import scala.Tuple2;

public class Demo01batchJ {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataSource<String> stringDataSource = env.readTextFile("E:\\jinghang\\实训01\\Day10-1\\flink.txt");
    stringDataSource.flatMap(new FlatMapFunction<String, String>() {
        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
            String[] split = s.split(" ");
            for (String s1 : split) {
                collector.collect(s1);
            }
        }
    })
            .map(new MapFunction<String, Tuple2<String,Integer>>() {
                @Override
                public Tuple2<String, Integer> map(String s) throws Exception {
                    return new Tuple2<>(s,1);
                }
            })

            .groupBy(0)

            .sum(1)
            .print();
    env.execute("WC job");
    }
}
