package com.jinghang.Day02;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo01environmentJ {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        LocalStreamEnvironment lenv = StreamExecutionEnvironment.createLocalEnvironment();

        StreamExecutionEnvironment renv = StreamExecutionEnvironment.createRemoteEnvironment("", 9999);

        env.setParallelism(1);
        env.disableOperatorChaining();

    }
}
