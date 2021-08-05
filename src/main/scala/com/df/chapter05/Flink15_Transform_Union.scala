package com.df.chapter05

import org.apache.flink.streaming.api.scala._

object Flink15_Transform_Union {
    def main(args: Array[String]): Unit = {

        // 1. 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 2. 从集合中读取流
        val numDS1: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4))
        val numDS2: DataStream[Int] = env.fromCollection(List(7, 8, 9, 10))
        val numDS3: DataStream[Int] = env.fromCollection(List(17, 18, 19, 110))


        val result: DataStream[Int] = numDS1.union(numDS2).union(numDS3)

        //result.print()
        numDS1.print()

        env.execute()
    }
}
