package com.df.chapter05

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object Flink24_Case_PVByProcess {
    def main(args: Array[String]): Unit = {

        //1. 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //2. 读取数据
        val logDS: DataStream[String] = env.readTextFile("input/UserBehavior.csv")
        //转换为样例类
        val userBehaviorDS: DataStream[UserBehavior] = logDS.map(
            line => {
                val data: Array[String] = line.split(",")
                UserBehavior(
                    data(0).toLong,
                    data(1).toLong,
                    data(2).toInt,
                    data(3),
                    data(4).toLong
                )
            }
        )

        //3. 处理逻辑
        //3.1 过滤数据
        val filterDS: DataStream[UserBehavior] = userBehaviorDS.filter(a => a.behavior == "pv")

        //3.2分组计数
        filterDS
          .keyBy(_.behavior)
          .process(
              new KeyedProcessFunction[String, UserBehavior, Long] {

                  var pvCount: Long = 0L

                  override def processElement(value: UserBehavior, ctx: KeyedProcessFunction[String, UserBehavior, Long]#Context, out: Collector[Long]): Unit = {

                      pvCount += 1L

                      out.collect(pvCount)
                  }
              }
          ).print("pv by process")

        env.execute()

    }

    case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
}
