package com.df.chapter06

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time

object Flink03_Window_AggFunction01 {
    def main(args: Array[String]): Unit = {

        // 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 读取数据
        val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

        // 处理数据
        // 转成样例类
        val waterSensorDS: DataStream[WaterSensor] = socketDS.map(
            line => {
                val lines: Array[String] = line.split(",")
                WaterSensor(lines(0), lines(1).toLong, lines(2).toInt)
            }
        )

        //        waterSensorDS
        //          .map(data => (data.id, data.vc))
        //          .keyBy(0)
        //          .reduce(new ReduceFunction[(String, Int)] {
        //              override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
        //                  (value1._1, value1._2 + value2._2)
        //              }
        //          })
        //            .print()

        waterSensorDS
          .map(a => (a.id, a.vc))
          .keyBy(0)
          .timeWindow(Time.seconds(3))
          .aggregate(new AggregateFunction[(String, Int), Long, Long] {
              override def createAccumulator(): Long = {
                  0
              }

              override def add(value: (String, Int), accumulator: Long): Long = {
                  value._2 + accumulator
              }

              override def getResult(accumulator: Long): Long = {
                  accumulator
              }

              override def merge(a: Long, b: Long): Long = {
                  a + b
              }
          })
          .print()

        // 开始执行
        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

}
