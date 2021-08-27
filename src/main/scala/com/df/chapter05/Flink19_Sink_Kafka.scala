package com.df.chapter05

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object Flink19_Sink_Kafka {
    def main(args: Array[String]): Unit = {

        // 1. 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 2. 读取数据
        val sensorDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

        // 3. 处理数据
        sensorDS.addSink(
            new FlinkKafkaProducer011[String](
                "",
                "flink_test",
                new SimpleStringSchema()
            )
        )

        // 4. 开始执行
        env.execute()
    }
}
