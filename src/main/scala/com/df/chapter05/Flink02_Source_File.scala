package com.df.chapter05

import org.apache.flink.streaming.api.scala._

object Flink02_Source_File {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val sensor: DataStream[String] = env.readTextFile("input/sensor-data.log")

        sensor.print()

        env.execute()

    }
}
