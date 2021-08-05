package com.df.chapter05

import org.apache.flink.streaming.api.scala._

object Flink14_Transform_Connect {
    def main(args: Array[String]): Unit = {
        // 1.创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(2)

        // 2.读取数据
        val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

        // 3.转换成样例类
        val mapDS: DataStream[WaterSensor] = sensorDS.map(
            lines => {
                val datas: Array[String] = lines.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )

        val numDS: DataStream[Int] = env.fromCollection(List(1,2,3,4,5,6))


        val resultDS = mapDS.connect(numDS)

        resultDS.map(
            s => s.id,
            num => num + 1
        )

            env.execute()

    }


    case class WaterSensor(id: String, ts: Long, vc: Int)

}
