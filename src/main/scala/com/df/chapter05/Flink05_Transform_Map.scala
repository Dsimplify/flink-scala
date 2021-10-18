package com.df.chapter05

import org.apache.flink.streaming.api.scala._

object Flink05_Transform_Map {
    def main(args: Array[String]): Unit = {

        //1. 创建环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //2. 读取数据
        val lineDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

        //3. 处理逻辑
        val result: DataStream[WaterSensor] = lineDS.map(
            lines => {
                val list: Array[String] = lines.split(",")
                WaterSensor(list(0), list(1).toLong, list(2).toInt)
            }
        )
        result.print()

        //4. 开始执行
        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

}
