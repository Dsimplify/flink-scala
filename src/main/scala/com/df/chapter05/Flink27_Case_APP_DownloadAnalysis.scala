package com.df.chapter05

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object Flink27_Case_APP_DownloadAnalysis {
    def main(args: Array[String]): Unit = {

        // 1.创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 2.读取数据
        val inputDS = env.addSource(new MySourceFunction())

        val filterDS: DataStream[MarketingUserBehavior] = inputDS.filter(_.behavior == "DOWNLOAD")

        filterDS
            .map(a => (a.channel, 1))
            .keyBy(_._1)
            .sum(1)
            .print()


        inputDS.print()

        env.execute()

    }

    class MySourceFunction extends SourceFunction[MarketingUserBehavior] {

        var flag = true
        //
        val userBehaviorList = List("DOWNLOAD", "INSTALL", "UPDATE", "UNINSTALL")
        val channelList = List("HUAWEI", "XIAOMI", "OPPO", "VIVO")

        override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
            while (flag) {
                ctx.collect(
                    MarketingUserBehavior(
                        Random.nextInt(100).toLong,
                        userBehaviorList(Random.nextInt(userBehaviorList.length)),
                        channelList(Random.nextInt(channelList.size)),
                        System.currentTimeMillis()
                    )
                )
                Thread.sleep(1000L)
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
