package com.df.chapter05

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object Flink27_Case_APP_DownloadAnalysis01 {
    def main(args: Array[String]): Unit = {

        // 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 读取数据
        val datas: DataStream[UserBehavior] = env.addSource(new MySource)

        // 处理逻辑
        datas
          .filter(_.behavior == "DOWNLOAD")
          .map(d => (d.channel, 1))
          .keyBy(_._1)
          .sum(1)
          .print()


        // 开始执行
        env.execute()
    }

    // 自定义数据
    class MySource extends SourceFunction[UserBehavior] {

        private val a = List("DOWNLOAD", "CLICK", "INSTALL", "UNINSTALL")
        private val b = List("huawei", "xiaomi", "apple", "honor")
        private var flag = true

        override def run(ctx: SourceFunction.SourceContext[UserBehavior]): Unit = {
            while (flag) {
                ctx.collect(
                    UserBehavior(
                        Random.nextInt(100).toLong,
                        a(Random.nextInt(a.length)),
                        b(Random.nextInt(b.length)),
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

    // 数据样例类
    case class UserBehavior(userId: Long, behavior: String, channel: String, timestamp: Long)

}
