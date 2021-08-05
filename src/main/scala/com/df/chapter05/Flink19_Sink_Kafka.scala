package com.df.chapter05

import org.apache.flink.streaming.api.scala._

object Flink19_Sink_Kafka {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val inputDS: DataStream[String] = env.socketTextStream("localhost", 9999)


    }
}
