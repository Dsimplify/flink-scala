package com.df.chapter05

import org.apache.flink.streaming.api.scala._



object Flink23_Case_PV {
    def main(args: Array[String]): Unit = {

        // 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 读取数据
        val userBehavior: DataStream[String] = env.readTextFile("input/UserBehavior.csv")

        // 处理逻辑
        val user: DataStream[UserBehavior01] = userBehavior.map(
            data => {
                val strings: Array[String] = data.split(",")
                UserBehavior01(
                    strings(0).toInt,
                    strings(1).toInt,
                    strings(2).toInt,
                    strings(3),
                    strings(4).toLong
                )
            }
        )
        user.filter(b => b.behavior == "pv")
          .map(d => ("pv", 1))
          .keyBy(_._1)
          .sum(1)
          .print()

        // 开始执行
        env.execute()
    }

    /**
     * UserBehavior的样例类
     */
    case class UserBehavior01(userId: Int, itemId: Int, categoryId: Int, behavior: String, timestamp: Long)
}


