package com.df.chapter05

import org.apache.flink.streaming.api.scala._

object Flink08_Transform_FlatMap {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val list: DataStream[List[Int]] = env.fromCollection(
            List(
                List(1, 2, 3, 4),
                List(5, 6, 7)
            )
        )
        val result: DataStream[Int] = list.flatMap(list => list)

        result.print()

        env.execute()

    }
}
