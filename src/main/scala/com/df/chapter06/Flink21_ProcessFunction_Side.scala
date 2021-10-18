package com.df.chapter06


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector


object Flink21_ProcessFunction_Side {
    def main(args: Array[String]): Unit = {

        // 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 设置并行度
        env.setParallelism(1)

        // 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 读取数据
        val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)


        // 封装数据
        val sensorDS: DataStream[WaterSensor] = socketDS.map(
            line => {
                val datas: Array[String] = line.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        ).assignTimestampsAndWatermarks(
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
        sensorDS
            .keyBy(_.id)
            .process(
                new KeyedProcessFunction[String, WaterSensor, String] {
                    override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {
                        if (value.vc > 5) {
                            val highTag = new OutputTag[WaterSensor]("high")
                            ctx.output(highTag, value)
                        } else {
                            out.collect("" + value)
                        }
                    }
                }
            )
            .print()



        // 开始执行
        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)
}
