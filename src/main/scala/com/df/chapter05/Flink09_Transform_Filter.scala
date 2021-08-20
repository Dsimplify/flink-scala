package com.df.chapter05

import org.apache.flink.streaming.api.scala._

object Flink09_Transform_Filter {
    def main(args: Array[String]): Unit = {

        // 1. 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 2. 读取数据
        val list: DataStream[Int] = env.fromCollection(
            List(1, 2, 3, 4, 5, 6, 7)
        )

        // 3. 处理数据逻辑
        list.filter(a => a % 2 == 0).print()


        // 4. 开始执行
        env.execute()

    }
}
