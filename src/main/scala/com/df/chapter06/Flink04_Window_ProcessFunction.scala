package com.df.chapter06


import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink04_Window_ProcessFunction {
    def main(args: Array[String]): Unit = {

        // 1.创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 2.读取数据
        val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

        // 3.处理数据
        val dataWS: WindowedStream[(String, Int), String, TimeWindow] = socketDS
          .map(data => (data, 1))
          .keyBy(_._1)
          .timeWindow(Time.seconds(3))

        dataWS.process(
            new ProcessWindowFunction[(String, Int), String, String, TimeWindow] {
                override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
                    out.collect(
                        "当前的key是=" + key
                          + "，属于窗口[" + context.window.getStart + "," + context.window.getEnd
                          + "],一共有数据" + elements.size + "条数据"
                    )
                }
            }).print()

        // 4.执行
        env.execute()
    }
}
