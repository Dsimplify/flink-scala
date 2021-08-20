package com.df.chapter02

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object Flink05_WC_OperatorChains {
    def main(args: Array[String]): Unit = {

        // 1.创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 2.读取数据
        val socket: DataStream[String] = env.socketTextStream("localhost",9999)

        // 3.执行逻辑
        val WordDS: DataStream[String] = socket.flatMap(a => a.split(" ")).setParallelism(2)

        val mapDS: DataStream[(String, Int)] = WordDS.map((_, 1)).setParallelism(2)

        val result: DataStream[(String, Int)] = mapDS.keyBy(0).sum(1)

        // 4.打印结果
        result.print()

        // 5.启动执行环境
        env.execute()

    }

}
