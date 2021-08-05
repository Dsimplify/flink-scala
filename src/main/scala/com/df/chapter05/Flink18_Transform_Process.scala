package com.df.chapter05

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object Flink18_Transform_Process {
    def main(args: Array[String]): Unit = {

        // 1.创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 2.读取数据
        val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")
        // 3.转成样例类
        val mapDS: DataStream[WaterSensor] = sensorDS.map(
            lines => {
                val datas: Array[String] = lines.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )

       val keyDS: KeyedStream[WaterSensor, String] = mapDS.keyBy(_.id)

        keyDS.print()

        keyDS.process(new MyKeyedProcessFunction).print()



        env.execute()

    }

    class MyKeyedProcessFunction extends KeyedProcessFunction[String, WaterSensor, String] {
        override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {
            out.collect("i am coming, 分组的key是=" + ctx.getCurrentKey + ", 数据=" + value)
        }
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)
}
