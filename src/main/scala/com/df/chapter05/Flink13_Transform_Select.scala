package com.df.chapter05

import org.apache.flink.streaming.api.scala._

object Flink13_Transform_Select {
    def main(args: Array[String]): Unit = {

        // 1. 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 2. 读取数据
        val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

        // 3. 数据处理逻辑
        val mapDS: DataStream[WaterSensor] = sensorDS.map(a => {
            val datas: Array[String] = a.split(",")
            WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
        })

        val splitSS: SplitStream[WaterSensor] = mapDS.split(
            sensor => {
                if (sensor.vc < 50) {
                    Seq("normal","hahahaha")
                } else if (sensor.vc < 80) {
                    Seq("Warn")
                } else {
                    Seq("alarm")
                }
            }
        )

        splitSS.select("Warn").print()
//
//        splitSS.select("hahahaha").print("normal")
//        splitSS.select("Warn").print("Warn")
//        splitSS.select("alarm").print("alarm")

        // 4. 开始执行
        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)
}
