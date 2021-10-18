package com.df.chapter05

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object Flink28_Case_APP_MarketingAnalysis {
    def main(args: Array[String]): Unit = {

        // 1.创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 2.读取数据
        val sourceDS: DataStream[MarketingUserBehavior] = env.addSource(new MySourceFunction)

        // 3.处理数据
        sourceDS
          .map(a => (a.behavior, 1))
          .keyBy(_._1)
          .sum(1)
          .print()

        // 4.执行
        env.execute()
    }

    class MySourceFunction extends SourceFunction[MarketingUserBehavior] {

        var flag = true
        val userBehavior = List("DOWNLOAD", "INSTALL", "UPDATE", "UNINSTALL")
        val userChannel = List("HUAWEI", "XIAOMI", "OPPO", "VIVO")

        override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
            while (flag) {
                ctx.collect(
                    MarketingUserBehavior(
                        Random.nextInt(100).toLong,
                        userBehavior(Random.nextInt(userBehavior.size)),
                        userChannel(Random.nextInt(userChannel.size)),
                        System.currentTimeMillis()
                    )
                )
                Thread.sleep(500)
            }
        }

        override def cancel(): Unit = {
            flag = false
        }
    }

    case class MarketingUserBehavior(
                                      userId: Long,
                                      behavior: String,
                                      channel: String,
                                      timestamp: Long)

}
