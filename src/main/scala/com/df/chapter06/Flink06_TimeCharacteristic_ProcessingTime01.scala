package com.df.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Flink06_TimeCharacteristic_ProcessingTime01 {
    def main(args: Array[String]): Unit = {

        // 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 读取数据
        val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

        // 处理数据
        // 转化为样例类
        val sensorDS: DataStream[WaterSensor] = socketDS.map(
            line => {
                val lines: Array[String] = line.split(",")
                WaterSensor(lines(0), lines(1).toLong, lines(2).toInt)
            }
        )

        sensorDS
          .map(a => (a.id, a.vc))
          .keyBy(_._1)
          .timeWindow(Time.seconds(10))
          .sum(1)
          .print()


        // 执行
        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

}
