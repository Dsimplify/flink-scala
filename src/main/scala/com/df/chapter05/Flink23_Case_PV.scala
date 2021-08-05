package com.df.chapter05

import org.apache.flink.streaming.api.scala._

object Flink23_Case_PV {
    def main(args: Array[String]): Unit = {

        //1. 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //2. 读取数据
        val logDS: DataStream[String] = env.readTextFile("input/UserBehavior.csv")

        //转换为样例类
        val caseDS: DataStream[UserBehavior] = logDS.map(
            logs => {
                val data: Array[String] = logs.split(",")
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
        //3.1 过滤出pv的数据
        val pvDS: DataStream[UserBehavior] = caseDS.filter(a => a.behavior == "pv")

        //3.2 转换数据格式
        val mapDS: DataStream[(String, Int)] = pvDS.map(a => ("pv", 1))

        //3.3 分组
        val keyDS: KeyedStream[(String, Int), String] = mapDS.keyBy(_._1)

        //3.4 聚合
        val result: DataStream[(String, Int)] = keyDS.sum(1)

        //4.打印
        result.print()

        //5.执行
        env.execute()

    }

    case class UserBehavior (userId: Long,
                             itemId: Long,
                             categoryId: Int,
                             behavior: String,
                             timestamp: Long)
}
