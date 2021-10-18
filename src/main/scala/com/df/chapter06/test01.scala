package com.df.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object test01 {
    def main(args: Array[String]): Unit = {

        // 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(1)
        // 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 读取数据,并封装成样例类
        val userBehaviorDS: DataStream[UserBehavior] = env
          .readTextFile("input/UserBehavior.csv")
          .map(
              line => {
                  val datas: Array[String] = line.split(",")
                  UserBehavior(
                      datas(0).toLong,
                      datas(1).toLong,
                      datas(2).toInt,
                      datas(3),
                      datas(4).toLong
                  )
              }
          )
            .assignAscendingTimestamps(_.timestamp * 1000L)

        userBehaviorDS
          .filter(_.behavior == "pv")
          .map(d => ("pv", 1L))
          .keyBy(_._1)
          .timeWindow(Time.hours(1))
          .sum(1)
          .print()

        // 开始执行
        env.execute()
    }

    case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
}
