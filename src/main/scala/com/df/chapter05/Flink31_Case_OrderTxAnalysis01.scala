package com.df.chapter05

import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

object Flink31_Case_OrderTxAnalysis01 {
    def main(args: Array[String]): Unit = {

        // 创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 读取数据
        // 订单数据
        val orderDS: DataStream[Order] = env
          .readTextFile("input/OrderLog.csv")
          .map(
              d => {
                  val data: Array[String] = d.split(",")
                  Order(
                      data(0).toLong,
                      data(1),
                      data(2),
                      data(3).toLong
                  )
              }
          )

        // 交易数据
        val reDS: DataStream[Receipt] = env
          .readTextFile("input/ReceiptLog.csv")
          .map(
              d => {
                  val data: Array[String] = d.split(",")
                  Receipt(
                      data(0),
                      data(1),
                      data(2).toLong
                  )
              }
          )

        // 处理逻辑
        // 合并两条流
        val connDS: ConnectedStreams[Order, Receipt] = orderDS.connect(reDS)

        connDS
          .keyBy(a => a.txId, b => b.txID)
          .process(new MyCoProcessFunction)
          .print()



        // 开始执行
        env.execute()
    }

    class MyCoProcessFunction extends CoProcessFunction[Order, Receipt, String] {

        // 存储订单状态
        private val orderMap: mutable.Map[String, Order] = mutable.Map()

        // 存储交易状态
        private val reMap: mutable.Map[String, Receipt] = mutable.Map()

        // 订单数据进入到这个方法
        override def processElement1(value: Order, ctx: CoProcessFunction[Order, Receipt, String]#Context, out: Collector[String]): Unit = {
            val maybeReceipt: Option[Receipt] = reMap.get(value.txId)
            if (maybeReceipt.isEmpty) {
                orderMap.put(value.txId, value)
            } else {
                out.collect("订单" + value.orderId + "对账成功")
                reMap.remove(value.txId)
            }
        }

        // 交易数据
        override def processElement2(value: Receipt, ctx: CoProcessFunction[Order, Receipt, String]#Context, out: Collector[String]): Unit = {
            val maybeOrder: Option[Order] = orderMap.get(value.txID)
            if (maybeOrder.isEmpty) {
                reMap.put(value.txID, value)
            } else {
                out.collect("订单" + maybeOrder.get.orderId + "对账成功")
                orderMap.remove(value.txID)
            }
        }
    }


    /**
     *
     * @param orderId   订单ID
     * @param eventType 操作类型
     * @param txId      交易码
     * @param eventTime 交易时间戳
     */
    case class Order(orderId: Long,
                     eventType: String,
                     txId: String,
                     eventTime: Long)


    /**
     *
     * @param txID       交易码
     * @param payChannel 支付方式
     * @param eventTime  交易时间戳
     */
    case class Receipt(txID: String,
                       payChannel: String,
                       eventTime: Long)

}
