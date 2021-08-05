package com.df.chapter02

import org.apache.flink.streaming.api.scala._

object Flink02_WC_BoundedStream {
    def main(args: Array[String]): Unit = {

        //1. 创建环境（流处理）
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//        env.setParallelism(1)

        //2. 读取数据
        val data1: DataStream[String] = env.readTextFile("input/word.txt")

        //3. 处理数据
        val data2: DataStream[(String, Int)] = data1.flatMap(d => d.split(" ")).map(d => (d, 1)).keyBy(0).sum(1)

        data2.print()

        //4. 启动
        env.execute()
    }
}
