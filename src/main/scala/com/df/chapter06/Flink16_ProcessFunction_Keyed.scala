package com.df.chapter06

import com.df.chapter06.Flink10_WaterMark_Punctuated.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector



object Flink16_ProcessFunction_Keyed {
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
        .assignAscendingTimestamps(_.ts * 1000L)

        waterDS.keyBy(_.id)
            .process(
                new KeyedProcessFunction[String, WaterSensor, String] {

                    private var currentHeight = 0

                    private var alarmTimer = 0L

                    override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {
                        if (value.vc > currentHeight) {
                            println("》》》传感器=" + ctx.getCurrentKey + ",当前保存的currentHeigh=" + currentHeight)
                            if (alarmTimer == 0) {
                                alarmTimer = value.ts * 1000L + 5000L
                                ctx.timerService().registerEventTimeTimer(alarmTimer)
                            }
                        } else {
                            ctx.timerService().deleteEventTimeTimer(alarmTimer)
                            alarmTimer = 0L
                        }

                        currentHeight = value.vc

                        println(alarmTimer)
                        println(currentHeight)
                    }

                    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
                        println("传感器=" + ctx.getCurrentKey + ",当前保存的currentHeight=" + currentHeight)
                        out.collect(ctx.getCurrentKey + "在" + timestamp + "已经连续5s水位上升")
                    }
                }
            )
            .print()

        // 开始执行
        env.execute()
    }
}
