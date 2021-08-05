package com.df.chapter05

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object Flink07_Transform_RichMapFunction {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val socket: DataStream[String] = env.socketTextStream("localhost",9999)

        val result: DataStream[WaterSensor] = socket.map(new MyMapFunction)

        result.print()

        env.execute()
    }

    class MyMapFunction extends RichMapFunction[String, WaterSensor] {
        var count = 0

        override def map(value: String): WaterSensor = {
            val list: Array[String] = value.split(",")
            count += 1
            WaterSensor(getRuntimeContext.getTaskName, list(1).toLong, list(2).toInt)
        }

        override def open(parameters: Configuration): Unit = {
            println("open" + count)
        }

        override def close(): Unit = {
            println("close" + count)
        }
    }

    case class WaterSensor (id: String, ts: Long, vc: Int)
}
