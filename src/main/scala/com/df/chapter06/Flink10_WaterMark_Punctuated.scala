package com.df.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink10_WaterMark_Punctuated {
    def main(args: Array[String]): Unit = {

        // 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 指定时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 读取数据
        val sourceDS: DataStream[String] = env.socketTextStream("localhost", 9999)

        // 封装成样例类
        val waterDS: DataStream[WaterSensor] = sourceDS.map(
            d => {
                val line: Array[String] = d.split(",")
                WaterSensor(line(0), line(1).toLong, line(2).toInt)
            }
        )

        val value: DataStream[WaterSensor] = waterDS.assignTimestampsAndWatermarks(
            new AssignerWithPunctuatedWatermarks[WaterSensor] {
                override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long): Watermark = {
                    new Watermark(extractedTimestamp)
                }

                override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
                    element.ts * 1000L
                }
            }
        )

        // 处理逻辑
        value.map(d => (d.id, 1))
            .keyBy(_._1)
            .timeWindow(Time.seconds(5))
            .process(
                new ProcessWindowFunction[(String, Int), String, String, TimeWindow] {
                    override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
                        out.collect(
                            "当前的key是=" + key
                              + "，属于窗口[" + context.window.getStart + "," + context.window.getEnd
                              + "]，一共有" + elements.size + "条数据"
                              + "，当前的watermark=" + context.currentWatermark
                        )
                    }
                }
            )
            .print()



        // 开始执行
        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)
}
