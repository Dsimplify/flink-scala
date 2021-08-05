package com.df.chapter02

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object Flink05_WC_OperatorChains {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


        val socket: DataStream[String] = env.socketTextStream("localhost",9999)

        val WordDS: DataStream[String] = socket.flatMap(a => a.split(" ")).setParallelism(2)

        val mapDS: DataStream[(String, Int)] = WordDS.map((_, 1)).setParallelism(2)

        val result: DataStream[(String, Int)] = mapDS.keyBy(0).sum(1)

        result.print()

        env.execute()

    }

}
