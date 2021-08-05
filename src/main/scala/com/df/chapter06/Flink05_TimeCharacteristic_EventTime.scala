package com.df.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink05_TimeCharacteristic_EventTime {
    def main(args: Array[String]): Unit = {

        // window触发计算条件
        // 1. watermark时间（max_eventTime-t）>= window_end_time // t:延迟时间
        // 2. 在[window_start_time,window_end_time)区间中有数据存在，注意是左闭右开的区间

        // 1.创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 在执行环境里，指定时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        // 2.读取数据，转换为样例类
        val socketDS: DataStream[WaterSensor] = env
          .socketTextStream("localhost", 9999)
          .map(line => {
              val data: Array[String] = line.split(",")
              WaterSensor(data(0), data(1).toLong, data(2).toInt)
          })
          // Flink里的时间戳都是毫秒
          .assignAscendingTimestamps(_.ts * 1000L)

        // 3.处理数据
        socketDS
          //.map(data => (data.id, 1))
          .keyBy(_.id)
          .timeWindow(Time.seconds(5))
          .process(
              new ProcessWindowFunction[WaterSensor, String, String, TimeWindow] {
                  override def process(key: String, context: Context, elements: Iterable[WaterSensor], out: Collector[String]): Unit = {
                      out.collect("触发窗口计算" + "," + key + "," + context.currentWatermark +
                        "," + context.currentProcessingTime + "," + context.window)
                  }
              }
          )
          .print()


        // 4.执行
        env.execute()
    }

    case class WaterSensor (id: String, ts: Long, vc: Int)
}
