package com.df.chapter05

import org.apache.flink.streaming.api.scala._

object Flink08_Transform_FlatMap {
    def main(args: Array[String]): Unit = {

        // 1. 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 2.读取数据
        val list: DataStream[List[Int]] = env.fromCollection(
            Seq(
                List(1, 2, 3, 4),
                List(5, 6, 7)
            )
        )

        //list.print()

        // 3. 处理数据逻辑
        val result: DataStream[Int] = list.flatMap(list => list)

        // 4. 打印结果
        result.print()

        // 5. 开始执行
        env.execute()

    }
}
