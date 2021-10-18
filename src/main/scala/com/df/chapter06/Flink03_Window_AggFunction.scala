package com.df.chapter06

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object Flink03_Window_AggFunction {
    def main(args: Array[String]): Unit = {

        // 1.创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 2.读取数据
        val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

        // 3.处理数据
        val waterSensorDS: DataStream[WaterSensor] = socketDS.map(
            line => {
                val data: Array[String] = line.split(",")
                WaterSensor(
                    data(0),
                    data(1).toLong,
                    data(2).toInt
                )
            }
        )
        //
        //        waterSensorDS
        //          .map(data => (data.id, 1))
        //          .keyBy(_._1)
        //          .timeWindow(Time.seconds(10))
        //          .reduce(new ReduceFunction[(String, Int)] {
        //              override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
        //                  (value1._1, value1._2 + value2._2)
        //              }
        //          })
        //          .print()

        val aggDS: WindowedStream[(String, Int), String, TimeWindow] = waterSensorDS
          .map(data => (data.id, 1))
          .keyBy(_._1)
          .timeWindow(Time.seconds(10))


        // AggregateFunction是一个基于中间计算结果状态进行增量计算的函数。由于是迭代计算方式，
        // 所以，在窗口处理过程中，不用缓存整个窗口的数据，所以效率执行比较高。
        aggDS.aggregate(new AggregateFunction[(String, Int), Long, Long] {
            override def createAccumulator(): Long = 0L

            override def add(value: (String, Int), accumulator: Long): Long = {
                println("add......")
                value._2 + accumulator
            }

            override def getResult(accumulator: Long): Long = {
                println("get result")
                accumulator
            }

            override def merge(a: Long, b: Long): Long = {
                a + b
            }
        }).print()

        // 4.开始执行
        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

}
