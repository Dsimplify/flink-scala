package com.df.chapter05

import org.apache.flink.streaming.api.scala._

object Flink12_Transform_Split {
    def main(args: Array[String]): Unit = {


        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val sensor: DataStream[String] = env.readTextFile("input/sensor-data.log")

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
        splitSS

        env.execute()


    }

    case class WaterSensor(id: String, ts: Long, vc: Int)
}
