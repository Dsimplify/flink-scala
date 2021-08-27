package com.df.chapter05

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Flink24_Case_PVByProcess {
    def main(args: Array[String]): Unit = {

        //1. 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //2. 读取数据
        val logDS: DataStream[String] = env.readTextFile("input/UserBehavior.csv")
        //转换为样例类
        val userBehaviorDS: DataStream[UserBehavior02] = logDS.map(
            line => {
                val data: Array[String] = line.split(",")
                UserBehavior02(
                    data(0).toLong,
                    data(1).toLong,
                    data(2).toInt,
                    data(3),
                    data(4).toLong
                )
            }
        )

        // 过滤出行为为浏览的用户
        val userFilter: DataStream[UserBehavior02] = userBehaviorDS.filter(_.behavior == "pv")

        //
        userFilter
            .map(d => ("pv", 1))
            .keyBy(_._1)
            .process(
                new KeyedProcessFunction[String, (String, Int), Int] {

                    private val longs: ListBuffer[Long] = mutable.ListBuffer[Long]()
                    override def processElement(value: (String, Int), ctx: KeyedProcessFunction[String, (String, Int), Int]#Context, out: Collector[Int]): Unit = {
                        longs.append(value._2)
                        out.collect(longs.size)
                    }
                }
            )
            .print()




        // 开始执行
        env.execute()

    }

    case class UserBehavior02(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
}
