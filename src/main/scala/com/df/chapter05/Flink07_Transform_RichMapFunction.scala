package com.df.chapter05

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object Flink07_Transform_RichMapFunction {
    def main(args: Array[String]): Unit = {

        // 1. 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 2. 读取数据
        val socket: DataStream[String] = env.socketTextStream("localhost", 9999)

        // 3. 处理数据逻辑
        val result: DataStream[WaterSensor] = socket.map(new MyMapFunction)

        // 4. 打印结果
        result.print()

        // 5. 开始执行
        env.execute("s")
    }

    class MyMapFunction extends RichMapFunction[String, WaterSensor] {
        var count = 0

        override def map(value: String): WaterSensor = {
            val list: Array[String] = value.split(",")
            count += 1
            println(getRuntimeContext.getTaskName)
            println(getRuntimeContext.getTaskNameWithSubtasks)
            WaterSensor(getRuntimeContext.getTaskName, list(1).toLong, list(2).toInt)
        }

        override def open(parameters: Configuration): Unit = {

        }

        override def close(): Unit = {

        }
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

}
