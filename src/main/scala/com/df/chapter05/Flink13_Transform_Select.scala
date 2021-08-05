package com.df.chapter05

import org.apache.flink.streaming.api.scala._

object Flink13_Transform_Select {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

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

        splitSS.select("hahahaha").print("normal")
        splitSS.select("Warn").print("Warn")
        splitSS.select("alarm").print("alarm")

        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)
}
