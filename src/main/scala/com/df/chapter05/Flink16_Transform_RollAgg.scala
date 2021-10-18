package com.df.chapter05

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

object Flink16_Transform_RollAgg {
    def main(args: Array[String]): Unit = {

        // 1. 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //env.setParallelism(3)

        // 2. 读取数据
        val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

        // 3. 转成样例类
        val mapDS: DataStream[WaterSensor] = sensorDS.map(
            lines => {
                val datas: Array[String] = lines.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )

        val dataDS: DataStream[(String, Long, Int)] = mapDS.map(a => (a.id, a.ts, a.vc))
        val keyDS: KeyedStream[(String, Long, Int), String] = dataDS.keyBy(_._1)

        val minDS: DataStream[(String, Long, Int)] = keyDS.minBy(2)
        val maxDS: DataStream[(String, Long, Int)] = keyDS.max(2)

        val sumDS: DataStream[(String, Long, Int)] = keyDS.sum(2)

        //val keyDS: KeyedStream[WaterSensor, String] = mapDS.keyBy(_.id)

        // 来一条、算一次、输出一次

        keyDS.print()

        //minDS.print("min")
        //maxDS.print()
        //        sumDS.print("sum")

        env.execute()

    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

}
