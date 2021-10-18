package com.df.chapter06

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink04_Window_ProcessFunction01 {
    def main(args: Array[String]): Unit = {

        // 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 读取数据
        val dataDS: DataStream[String] = env.socketTextStream("localhost", 9999)

        // 处理逻辑
        // 转成样例类
        val ds: DataStream[WaterSensor] = dataDS.map(
            line => {
                val lines: Array[String] = line.split(",")
                WaterSensor(lines(0), line(1).toLong, lines(2).toInt)
            }
        )

        val value: WindowedStream[(String, Int), String, TimeWindow] = ds
          .map(a => (a.id, a.vc))
          .keyBy(_._1)
          .timeWindow(Time.seconds(3))

        //        value.trigger(new Trigger {
        //            override def onElement(element: (String, Int), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        //                //获取分区的状态变量
        //                var firstSeen: ValueState[Boolean] = ctx.getPartitionedState(new ValueStateDescriptor[Boolean]("firstSeen", Types.of[Boolean]))
        //                //每个窗口的第一个数据才会进入到里面设置回调事件的事件
        //                if(!firstSeen.value()){
        //                    //获取当前水位线
        //                    val t = ctx.getCurrentWatermark+(1000-(ctx.getCurrentWatermark % 1000))
        //                    //注册事件时间的回调事件，注册下一秒的事件
        //                    ctx.registerEventTimeTimer(t)
        //                    //注册窗口结束时的事件
        //                    ctx.registerEventTimeTimer(window.getEnd)
        //                    //关闭时间的注册，保证每一秒内的事件不重复注册
        //                    firstSeen.update(true)
        //                }
        //                TriggerResult.CONTINUE
        //            }
        //
        //            override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        //                TriggerResult.CONTINUE
        //            }
        //
        //            override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        //                if (time == window.getEnd) {
        //                    print("==========")
        //                    TriggerResult.FIRE_AND_PURGE
        //                } else {
        //                    val t: Long = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
        //                    if (t < window.getEnd) {
        //                        ctx.registerEventTimeTimer(t)
        //                    }
        //                }
        //
        //                TriggerResult.FIRE
        //            }
        //
        //            override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
        //                val firstSeen: ValueState[Boolean] = ctx
        //                  .getPartitionedState(
        //                      new ValueStateDescriptor[Boolean](
        //                          "firstSeen", classOf[Boolean]
        //                      )
        //                  )
        //                //注销之前设置的事件
        //                firstSeen.clear()
        //
        //            }
        //        })

        //
        //        value
        //          .process(
        //              new ProcessWindowFunction[(String, Int), String, String, TimeWindow] {
        //                  override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
        //                      out.collect("当前的key是=" + key
        //                        + "，属于窗口[" + context.window.getStart + "," + context.window.getEnd
        //                        + "],一共有数据" + elements.size + "条数据" )
        //                  }
        //              }
        //          )
        //          .print()


        // 开始执行
        env.execute()
    }


    case class WaterSensor(id: String, ts: Long, vc: Int)

}
