package com.df.chapter05

import org.apache.flink.streaming.api.scala._

object Flink30_Case_AdClickAnalysis {
    def main(args: Array[String]): Unit = {

        // 统计 各个省份 的 每个广告 的点击量

        // 1.创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 2.读取数据
        val dataDS: DataStream[String] = env.readTextFile("input/AdClickLog.csv")

        //转换为样例类
        val adDS: DataStream[AdClickLog] = dataDS.map(
            data => {
                val line: Array[String] = data.split(",")
                AdClickLog(
                    line(0).toLong,
                    line(1).toLong,
                    line(2),
                    line(3),
                    line(4).toLong
                )
            }
        )

        adDS
          .map(a => (a.province + "_" + a.adId, 1))
          .keyBy(_._1)
          .sum(1)
          .print()

        // 4.执行
        env.execute()
    }

    /**
     *
     * @param userId    用户ID
     * @param adId      广告ID
     * @param province  省份
     * @param city      城市
     * @param timestamp 时间戳
     */
    case class AdClickLog(
                           userId: Long,
                           adId: Long,
                           province: String,
                           city: String,
                           timestamp: Long)
}
