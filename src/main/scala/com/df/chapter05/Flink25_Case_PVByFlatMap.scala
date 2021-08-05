package com.df.chapter05

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

object Flink25_Case_PVByFlatMap{
    def main(args: Array[String]): Unit = {

        //1. 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //2. 读取数据
        val logDS: DataStream[String] = env.readTextFile("input/UserBehavior.csv")
        //转换为样例类
        logDS
          .flatMap(
            line => {
                val data: mutable.ArrayOps[String] = line.split(",")
                if (data(3) == "pv") {
                    List(("pv", 1L))
                }else {
                    Nil
                }
            }
          )
          .keyBy(_._1)
          .sum(1)
          .print()

        //3. 处理逻辑
        //3.1 过滤数据


        env.execute()

    }

    case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

}
