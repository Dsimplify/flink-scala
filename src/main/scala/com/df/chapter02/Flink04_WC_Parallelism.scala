package com.df.chapter02


import org.apache.flink.streaming.api.scala._

object Flink04_WC_Parallelism {
    def main(args: Array[String]): Unit = {
        // 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 读取数据
        val socket: DataStream[String] = env.socketTextStream("localhost", 9999)

        // 处理数据
        val wordDS: DataStream[String] = socket.flatMap(_.split(" ")).setParallelism(2)

        val mapDS: DataStream[(String, Int)] = wordDS.map((_, 1)).setParallelism(3)

        val result: DataStream[(String, Int)] = mapDS.keyBy(0).sum(1).setParallelism(2)

        result.print()

        // 启动执行环境
        env.execute()
    }
}
