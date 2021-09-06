package com.jinghang.Day04

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.jinghang.Day02.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object Demo2MysqlSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds: DataStream[String] = env.readTextFile("E:\\jinghang\\实训01\\Day12-3\\sensor.txt")
    val ds01 = ds.map(line => {
      val splits = line.split(",")
      SensorReading(splits(0).trim, splits(1).trim.toLong, splits(2).trim.toDouble)
    })
    ds01.addSink(new Mysqlsink)
    env.execute("job")
  }
}

class Mysqlsink extends RichSinkFunction[SensorReading] {
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn=  DriverManager.getConnection("jdbc:mysql://localhost:3306/flink?serverTimezone=UTC","root","wu555555")
    insertStmt=conn.prepareStatement("INSERT INTO temperatures(sensor,temps) values (?,?)")
    updateStmt=conn.prepareStatement("UPDATE temperatures set temps=? where sensor = ? ")
  }

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit ={
    updateStmt.setDouble(1,value.temperature)
    updateStmt.setString(2,value.sensorId)
    updateStmt.execute()
    if (updateStmt.getUpdateCount==0){
      insertStmt.setString(1,value.sensorId)
      insertStmt.setDouble(2,value.temperature)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    updateStmt.close()
    insertStmt.close()
    conn.close()
    super.close()
  }
}