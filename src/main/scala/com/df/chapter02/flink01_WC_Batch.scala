package com.df.chapter02

import org.apache.flink.api.scala._

object flink01_WC_Batch {
    def main(args: Array[String]): Unit = {

        //1. 创建执行环境（批处理）
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

        //2. 读取数据
        val data: DataSet[String] = env.readTextFile("input/word.txt")

        //3. 处理数据
        //3.1 切分
        val data1: DataSet[String] = data.flatMap(a => a.split(" "))

        //3.2 转换
        val data2: DataSet[(String, Int)] = data1.map(d => (d, 1))

        //3.3 分组
        val data3: GroupedDataSet[(String, Int)] = data2.groupBy(0)

        //3.4 求和
        val data4: AggregateDataSet[(String, Int)] = data3.sum(1)

        //wordAndOneGS.print()
        data4.print()

        //Thread.sleep(5000L)

        //4. 启动（批处理不需要执行启动操作）
        //env.execute()

    }
}
