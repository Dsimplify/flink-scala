package com.df.chapter06

import com.df.chapter06.Flink10_WaterMark_Punctuated.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink12_Window_WaterMark4 {
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
            new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(2)) {
                override def extractTimestamp(element: WaterSensor): Long = {
                    element.ts * 1000L
                }
            }
        )

        // 处理逻辑
        value.keyBy(_.id)
            .timeWindow(Time.seconds(5))
            .allowedLateness(Time.seconds(2))
            .process(
                new ProcessWindowFunction[WaterSensor, String, String, TimeWindow] {
                    override def process(key: String, context: Context, elements: Iterable[WaterSensor], out: Collector[String]): Unit = {
                        out.collect("触发窗口计算，窗口有["
                          + elements.toString()
                          + "]数据，watermark="
                          + context.currentWatermark)
                    }
                }
            )
            .print()

        // 执行
        env.execute()
    }
}
