package com.df.chapter02



import org.apache.flink.streaming.api.scala._

object Flink04_WC_Parallelism {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val socket: DataStream[String] = env.socketTextStream("localhost",9999)

        val wordDS: DataStream[String] = socket.flatMap(_.split(" ")).setParallelism(2)

        val mapDS: DataStream[(String, Int)] = wordDS.map((_, 1)).setParallelism(3)

        val result: DataStream[(String, Int)] = mapDS.keyBy(0).sum(1).setParallelism(2)

        result.print()

        env.execute()
    }
}
