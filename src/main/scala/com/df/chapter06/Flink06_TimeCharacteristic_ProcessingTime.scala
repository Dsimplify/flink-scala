package com.df.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Flink06_TimeCharacteristic_ProcessingTime {
    def main(args: Array[String]): Unit = {

        // 1.创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 指定时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

        // 2.读取数据
        val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

        // 转换为样例类
        val waterDS: DataStream[WaterSensor] = socketDS.map(
            line => {
                val data: Array[String] = line.split(",")
                WaterSensor(data(0), data(1).toLong, data(2).toInt)
            }
        )

        // 3.处理数据
        waterDS
          .map(data => (data.id, 1))
          .keyBy(_._1)
          .timeWindow(Time.seconds(5))
          .sum(1)
          .print()

        // 4.执行
        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

}
