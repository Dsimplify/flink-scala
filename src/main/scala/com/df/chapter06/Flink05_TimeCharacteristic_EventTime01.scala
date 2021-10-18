package com.df.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink05_TimeCharacteristic_EventTime01 {
    def main(args: Array[String]): Unit = {

        // 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 事件时间
        env.setParallelism(1)

        // 读取数据
        val lineDS: DataStream[String] = env.socketTextStream("localhost", 9999)

        val sensorDS: DataStream[WaterSensor] = lineDS.map(
            line => {
                val lines: Array[String] = line.split(",")
                WaterSensor(lines(0), lines(1).toLong, lines(2).toInt)
            }
        )
          .assignAscendingTimestamps(t => t.ts * 1000L)

        sensorDS.keyBy(_.id)
          .timeWindow(Time.seconds(5))
          .process(
              new ProcessWindowFunction[WaterSensor, String, String, TimeWindow] {
                  override def process(key: String, context: Context, elements: Iterable[WaterSensor], out: Collector[String]): Unit = {
                      out.collect("触发计算, 处理时间：" + context.currentProcessingTime + ", watermark:" + context.currentWatermark + ",窗口时间[" + context.window.getStart + "," + context.window.getEnd + "]")
                  }
              }
          )
          .print()

        // 执行
        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

}
