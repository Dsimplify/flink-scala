package com.df.chapter05

import java.lang.Thread

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object Flink04_Source_MySource {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val result: DataStream[WaterSensor] = env.addSource(new MySourceFunction)

        result.print()

        env.execute()
    }

    class MySourceFunction extends  SourceFunction[WaterSensor] {

        var flag = true
        override def run(sourceContext: SourceFunction.SourceContext[WaterSensor]): Unit = {
            while (flag) {
                sourceContext.collect(
                    WaterSensor("sensor_" + (Random.nextInt(3) + 1), System.currentTimeMillis(), 40 + Random.nextInt(10))
                )
                Thread.sleep(1000L)
            }
        }

        override def cancel(): Unit = {
            flag = false
        }
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

}
