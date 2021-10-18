package com.df.chapter06

import java.lang

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink07_WaterMark_OutOfOrderness {
    def main(args: Array[String]): Unit = {

        // 1. 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 2. 读取数据
        val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

        // 3. 处理数据

        // 3.1 封装成样例类
        socketDS.map(
            line => {
                val lines: Array[String] = line.split(",")
                WaterSensor(lines(0), lines(1).toLong, lines(2).toInt)
            }
        )
          .assignTimestampsAndWatermarks(
              new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(5)) {
                  override def extractTimestamp(element: WaterSensor): Long = {
                      element.ts * 1000L
                  }
              }
          )

          .map(d => (d.id, d.vc))
          .keyBy(_._1)
          .timeWindow(Time.seconds(5))
          .process(
              new ProcessWindowFunction[(String, Int), String, String, TimeWindow] {
                  override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
                      out.collect("当前的key是=" + key
                        + "，属于窗口[" + context.window.getStart + "," + context.window.getEnd
                        + "]，一共有" + elements.size + "条数据"
                        + "，当前的watermark=" + context.currentWatermark)
                  }
              }
          )
          //            .sum(1)
          .print()

        // 4. 打印结果


        // 5. 执行
        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

}
