package com.df.chapter05

import java.lang.Thread

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object Flink04_Source_MySource {
    def main(args: Array[String]): Unit = {

        // 1. 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置执行并行度
        env.setParallelism(1)

        // 2. 处理数据
        // 自定义数据源
        val result: DataStream[WaterSensor] = env.addSource(new MySourceFunction)

        // 3. 打印结果
        result.print()

        // 4. 执行
        env.execute("s")
    }

    // 自定义数据源
    class MySourceFunction extends SourceFunction[WaterSensor] {
        var flag = true
        override def run(ctx: SourceFunction.SourceContext[WaterSensor]): Unit = {
            while (flag) {
                ctx.collect(
                    WaterSensor(
                        "s" + new Random().nextInt(3),
                        1577844111,
                        new Random().nextInt(5) + 40
                    )
                )
                Thread.sleep(100)
            }
        }

        override def cancel(): Unit = {
            flag = false
        }
    }


    case class WaterSensor(id: String, ts: Long, vc: Int)

}
