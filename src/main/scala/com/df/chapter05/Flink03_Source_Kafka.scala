package com.df.chapter05

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.api.scala._

object Flink03_Source_Kafka {
    def main(args: Array[String]): Unit = {

        // 1. 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 2. 读取数据
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "172.23.108.3:9092")
        properties.setProperty("group.id", "consumer")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset", "earliest")

        // 3. 处理数据
        val kafkaDS: DataStream[String] = env.addSource(
            new FlinkKafkaConsumer011[String](
                "flink_test",
                new SimpleStringSchema(),
                properties
            )
        )


        //kafkaDS.writeUsingOutputFormat()

        // 4. 开始执行
        env.execute("flink_test")
    }
}
