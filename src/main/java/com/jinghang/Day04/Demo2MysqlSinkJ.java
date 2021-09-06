package com.jinghang.Day04;

import com.jinghang.Day02.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Demo2MysqlSinkJ {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<String> filedss = env.readTextFile("E:\\jinghang\\实训01\\Day12-3\\sensor.txt");

        SingleOutputStreamOperator<SensorReading> mapds = filedss.map(line -> new SensorReading(
                line.split(",")[0].trim(),
                Long.parseLong(line.split(",")[1].trim()),
                Double.parseDouble(line.split(",")[2].trim())
        ));
        mapds.addSink(new MysqlsinkJ());
        env.execute("job");
    }


    static class MysqlsinkJ extends RichSinkFunction<SensorReading>{
        Connection conn=null;
        PreparedStatement updateStmt=null;
        PreparedStatement insertStmt=null;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn=DriverManager.getConnection("jdbc:mysql://localhost:3306/flink?serverTimezone=UTC","root","wu555555");
            insertStmt=conn.prepareStatement("INSERT INTO temperatures(sensor,temps) values (?,?)");
            updateStmt=conn.prepareStatement("UPDATE temperatures set temps= ? where sensor = ? ");
        }



        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
                updateStmt.setDouble(1,value.temperature());
                updateStmt.setString(2,value.sensorId());
                updateStmt.execute();
                if (updateStmt.getUpdateCount()==0){
                    insertStmt.setString(1,value.sensorId());
                    insertStmt.setDouble(2,value.temperature());
                    insertStmt.execute();
                }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            conn.close();
            super.close();
        }
    }

}
