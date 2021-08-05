package com.df.chapter05

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

object Flink06_Transform_MapFunction {
    def main(args: Array[String]): Unit = {

        //1. 创建环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //2. 读取数据
        val lineDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

        //3. 处理逻辑
        val result: DataStream[WaterSensor] = lineDS.map(new MyMapFunction)

        result.print()

        //4. 开始执行
        env.execute()
    }

    class MyMapFunction extends MapFunction[String, WaterSensor] {
        override def map(value: String): WaterSensor = {
            val datas: Array[String] = value.split(",")
            WaterSensor(datas(0),datas(1).toLong,datas(2).toInt)
        }
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)
}
