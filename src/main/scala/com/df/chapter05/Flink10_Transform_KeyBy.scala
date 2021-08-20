package com.df.chapter05

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._


object Flink10_Transform_KeyBy {
    def main(args: Array[String]): Unit = {

        // 1. 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 2. 读取数据
        val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

        // 3. 数据处理逻辑
        val mapDS: DataStream[WaterSensor] = sensorDS.map(a => {
            val lists: Array[String] = a.split(",")
            WaterSensor(lists(0), lists(1).toLong, lists(2).toInt)
        })

        //mapDS.print()
        //mapDS.keyBy(0).sum(2).print()

        val value: KeyedStream[WaterSensor, Tuple] = mapDS.keyBy(0)

        val value1: DataStream[WaterSensor] = value.min("id")

        //val value: DataStream[WaterSensor] = mapDS.keyBy(0).min(2)

//        val sensorKS: KeyedStream[WaterSensor, String] = mapDS.keyBy(
//            new KeySelector[WaterSensor, String] {
//                override def getKey(value: WaterSensor): String = {
//                    value.id
//                }
//            }
//        )

        value1.print()

        // 4. 开始执行
        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)
}
