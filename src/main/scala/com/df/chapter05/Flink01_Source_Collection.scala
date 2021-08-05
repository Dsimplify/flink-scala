package com.df.chapter05

import org.apache.flink.streaming.api.scala._

object Flink01_Source_Collection {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val listDS: DataStream[WaterSensor] = env.fromCollection(
            List(
                WaterSensor("ws_001", 1577844001, 45),
                WaterSensor("ws_002", 1577844015, 43),
                WaterSensor("ws_003", 1577844020, 42)
            )
        )

        listDS.print()
        env.execute()
    }
    case class WaterSensor(id: String, ts: Long, vc: Int)
}
