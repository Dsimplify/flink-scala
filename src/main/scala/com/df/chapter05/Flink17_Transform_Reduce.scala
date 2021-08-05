package com.df.chapter05

import org.apache.flink.streaming.api.scala._

object Flink17_Transform_Reduce {
    def main(args: Array[String]): Unit = {
        // 1.创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(2)

        // 2.读取数据
        val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")
        // 3.转成样例类
        val mapDS: DataStream[WaterSensor] = sensorDS.map(
            lines => {
                val datas: Array[String] = lines.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )

        // Flink的数据模型不是基于key-value对的。
        // 因此，不需要将数据集类型物理打包为键和值。
        // key是“虚拟的”：它们被定义为指导分组操作符的实际数据上的函数。
        val keyDS: KeyedStream[WaterSensor, String] = mapDS.keyBy(_.id)
        keyDS.print()

        keyDS.reduce(
            (a, b) => {
                println(a + "<===>" + b)
                WaterSensor(a.id, System.currentTimeMillis(), a.vc + b.vc)
            }
        ).print("reduce")

        env.execute()
    }
    case class WaterSensor(id: String, ts: Long, vc: Int)
}
