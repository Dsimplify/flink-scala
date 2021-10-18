package com.df.chapter05

import org.apache.flink.streaming.api.scala._

object Flink12_Transform_Split {
    def main(args: Array[String]): Unit = {

        // 1. 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 2. 读取数据
        val sensor: DataStream[String] = env.readTextFile("input/sensor-data.log")

        // 3. 数据处理逻辑
        val mapDS: DataStream[WaterSensor] = sensor.map(
            a => {
                val lines: Array[String] = a.split(",")
                WaterSensor(lines(0), lines(1).toLong, lines(2).toInt)
            }
        )
        val splitSS: SplitStream[WaterSensor] = mapDS.split(
            a => {
                if (a.vc < 50) {
                    Seq("normal")
                } else if (a.vc < 80) {
                    Seq("warn")
                } else {
                    Seq("alarm")
                }
            }
        )
        splitSS.print()

        // 4. 开始执行
        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

}
