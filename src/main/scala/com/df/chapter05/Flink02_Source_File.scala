package com.df.chapter05

import org.apache.flink.streaming.api.scala._

object Flink02_Source_File {
    def main(args: Array[String]): Unit = {

        // 1.创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(1)

        // 2.从文件读取数据
        val data: DataStream[String] = env.readTextFile("input/sensor-data.log")

        // 3.打印数据
        data.print()

        // 4.执行
        env.execute()
    }
}
