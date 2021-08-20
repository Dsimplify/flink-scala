package com.df.chapter05

import org.apache.flink.streaming.api.scala._

object Flink11_Transform_Shuffle {
    def main(args: Array[String]): Unit = {

        // 1. 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 2. 读取数据
        val dataDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

        // 3. 数据处理逻辑
        val shuffle: DataStream[String] = dataDS.shuffle

        // 4. 打印结果
        //dataDS.print("data")
        println()
        shuffle.print("shuffle")

        // 5. 开始执行
        env.execute()
    }
}
