package com.df.chapter05

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object Flink06_Transform_MapFunction {
    def main(args: Array[String]): Unit = {

        //1. 创建环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //2. 读取数据
        val lineDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

        //3. 处理逻辑
        val result: DataStream[WaterSensor] = lineDS.map(new MyMapFunction1)

        result.print()

        //4. 开始执行
        env.execute()
    }


    //
    class MyMapFunction extends MapFunction[String, WaterSensor] {
        override def map(value: String): WaterSensor = {
            val list: Array[String] = value.split(",")
            WaterSensor(list(0), list(1).toLong, list(2).toInt)
        }
    }

    //
    class MyMapFunction1 extends RichMapFunction[String, WaterSensor] {
        override def map(value: String): WaterSensor = {
            val list: Array[String] = value.split(",")
            WaterSensor(list(0), list(1).toLong, list(2).toInt)
        }

        override def open(parameters: Configuration): Unit = super.open(parameters)
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

}
