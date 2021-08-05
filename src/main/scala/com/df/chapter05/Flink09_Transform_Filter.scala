package com.df.chapter05

import org.apache.flink.streaming.api.scala._

object Flink09_Transform_Filter {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val list: DataStream[Int] = env.fromCollection(
            List(1, 2, 3, 4, 5, 6, 7)
        )
        list.filter(a => a % 2 == 0).print()

        env.execute()

    }
}
