package com.df.chapter05

import org.apache.flink.streaming.api.scala._

object Flink11_Transform_Shuffle {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val dataDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

        val shuffle: DataStream[String] = dataDS.shuffle

        dataDS.print("data")
        shuffle.print("shuffle")

        env.execute()
    }
}
