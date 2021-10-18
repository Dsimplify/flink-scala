package com.df.chapter05

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object Flink30_Case_AdClickAnalysis01 {
    def main(args: Array[String]): Unit = {

        // 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 读取数据
        val datas: DataStream[String] = env.readTextFile("input/AdClickLog.csv")

        // 处理逻辑
        // 封装成样例类
        val adDS: DataStream[Ad] = datas.map(
            d => {
                val data: Array[String] = d.split(",")
                Ad(data(0).toLong,
                    data(1).toLong,
                    data(2),
                    data(3),
                    data(4).toLong
                )
            }
        )

        adDS
          .map(d => (d.province + "_" + d.adID, 1))
          .keyBy(_._1)
          .sum(1)
          .print()

        // 开始执行
        env.execute()
    }

    /**
     *
     * @param userID    用户id
     * @param adID      广告id
     * @param province  省份
     * @param city      城市
     * @param timestamp 时间戳
     */
    case class Ad(userID: Long,
                  adID: Long,
                  province: String,
                  city: String,
                  timestamp: Long)

}
