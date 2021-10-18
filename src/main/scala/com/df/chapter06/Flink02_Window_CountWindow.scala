package com.df.chapter06

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

object Flink02_Window_CountWindow {
    def main(args: Array[String]): Unit = {

        // 1.创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 2.读取数据
        val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

        // 3.处理数据
        socketDS
          .flatMap(
              line => {
                  val lines: Array[String] = line.split(" ")
                  lines
              }
          )
          .map(data => (data, 1))
          .keyBy(_._1)
          .countWindow(3) // 滚动窗口
          //          .countWindow(3 ,2)  // 滑动窗口
          .sum(1)
          .print()



        // 4.开始执行
        env.execute()
    }
}
