package com.df.chapter06

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

object Flink01_Window_TimeWindow {
    def main(args: Array[String]): Unit = {

        // 1.创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 2.读取数据
        val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

        // 3.处理数据
        socketDS
          .flatMap(_.split(" "))
          .map(a => (a, 1))
          .keyBy(0)
          //          .timeWindow(Time.seconds(3))   // 滚动窗口
          //          .timeWindow(Time.seconds(5), Time.seconds(1))   // 滑动窗口
          .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3))) // 会话窗口
          .sum(1)
          .print()

        // 4.开始执行
        env.execute()

    }
}
