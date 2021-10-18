package com.df.chapter02

import org.apache.flink.streaming.api.scala._

object Flink03_WC_unBoundedStream {
    def main(args: Array[String]): Unit = {

        // 创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // 读取数据来源
        val socket = env.socketTextStream("localhost", 9999)

        // 对数据进行处理
        val lineDS: DataStream[String] = socket.flatMap(a => a.split(" "))

        val result = lineDS.map((_, 1))
          .keyBy(0)
          .sum(1)

        // 打印结果
        result.print()

        // 执行数据处理
        env.execute()
    }
}
