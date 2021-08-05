package com.df.chapter05

import org.apache.flink.streaming.api.scala._


object Flink10_Transform_KeyBy {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

        val mapDS: DataStream[WaterSensor] = sensorDS.map(a => {
            val lists: Array[String] = a.split(",")
            WaterSensor(lists(0), lists(1).toLong, lists(2).toInt)
        })

        mapDS.keyBy(a => a.id).print().setParallelism(2)

        //mapDS.keyBy(0).print()

        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)
}
