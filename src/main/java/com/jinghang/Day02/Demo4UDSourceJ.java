package com.jinghang.Day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class Demo4UDSourceJ {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Demo2SourceJ.SensorReading> dataStreamSource = env.addSource(new MySource());
        dataStreamSource.print("mysource");
        env.execute("job");
    }

    static class MySource implements SourceFunction<Demo2SourceJ.SensorReading>{
       Boolean running=true;
       Long mark=0l;
        @Override
        public void run(SourceContext sourceContext) throws Exception {
            while (running){
                String id=""+new Random().nextInt(6);
                long time=System.currentTimeMillis();
                double temp=new Random().nextGaussian()*20+40;
                new Demo2SourceJ.SensorReading(id,time,temp);
                mark++;
                if (mark>10){
                    cancel();
                }
            }
        }

        @Override
        public void cancel() {
            running=false;
        }
    }

}

