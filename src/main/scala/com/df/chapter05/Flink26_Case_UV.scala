package com.df.chapter05

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

object Flink26_Case_UV {
    def main(args: Array[String]): Unit = {

        //1. 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //2. 读取数据
        val logDS: DataStream[String] = env.readTextFile("input/UserBehavior.csv")
        //转换为样例类
        val userDS: DataStream[UserBehavior] = logDS.map(
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

        //3. 处理逻辑
        //3.1 过滤数据
        val filterDS: DataStream[UserBehavior] = userDS.filter(_.behavior == "pv")

        //3.2 转换数据
        val uvDS: DataStream[(String, Long)] = filterDS.map(a => ("uv", a.userId))

        //3.3 分组
        uvDS
          .keyBy(_._1)
          .process(new MyKeyedProcessFunction)
          .print()

        // 开始执行
        env.execute()

    }

    // 自己定义处理逻辑
    class MyKeyedProcessFunction extends KeyedProcessFunction[String, (String, Long), Long] {
        var uvCount = mutable.Set[Long]()

        override def processElement(value: (String, Long),
                                    ctx: KeyedProcessFunction[String, (String, Long), Long]#Context,
                                    out: Collector[Long]): Unit = {
            uvCount.add(value._2)
            out.collect(uvCount.size)
        }
    }

    // 样例类
    case class UserBehavior(userId: Long,
                            itemId: Long,
                            categoryId: Int,
                            behavior: String,
                            timestamp: Long)

}
