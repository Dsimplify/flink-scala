package com.df.chapter05

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object Flink22_Sink_MySQL {
    def main(args: Array[String]): Unit = {

        //1. 创建连接环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        //2. 读取数据
        val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

        val mapDS: DataStream[WaterSensor] = sensorDS.map(
            lines => {
                val datas: Array[String] = lines.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )
        mapDS.addSink(new MySinkFunction())
        //mapDS.print()

        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

    class MySinkFunction extends RichSinkFunction[WaterSensor] {
        var conn: Connection = _
        var pstmt: PreparedStatement = _

        override def open(parameters: Configuration): Unit = {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/my_test?serverTimezone=Asia/Shanghai"
                , "root", "123456")
            pstmt = conn.prepareStatement("insert into test1(id, ts, vc) VALUES(?,?,?)")
        }

        override def invoke(value: WaterSensor, context: SinkFunction.Context[_]): Unit = {
            pstmt.setString(1, value.id)
            pstmt.setLong(2, value.ts)
            pstmt.setInt(3, value.vc)
            pstmt.execute()
        }

        override def close(): Unit = {
            pstmt.close()
            conn.close()
        }
    }

}
