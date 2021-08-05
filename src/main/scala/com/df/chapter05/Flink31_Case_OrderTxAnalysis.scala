package com.df.chapter05

import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.immutable.HashMap
import scala.collection.mutable


/**
 *   业务系统 和 交易系统 实时对账
 */
object Flink31_Case_OrderTxAnalysis {
    def main(args: Array[String]): Unit = {

        // 1.创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 2.读取数据并转换为样例类
        //订单系统
        val orderDS: DataStream[OrderEvent] = env
          .readTextFile("input/OrderLog.csv")
          .map(
              line => {
                  val data: Array[String] = line.split(",")
                  OrderEvent(
                      data(0).toLong,
                      data(1),
                      data(2),
                      data(3).toLong
                  )
              }
          )

        //交易系统
        val txDS: DataStream[TxEvent] = env
          .readTextFile("input/ReceiptLog.csv")
          .map(
              line => {
                  val data: Array[String] = line.split(",")
                  TxEvent(
                      data(0),
                      data(1),
                      data(2).toLong
                  )
              }
          )

        // 3.处理数据
        val connDS: ConnectedStreams[OrderEvent, TxEvent] = orderDS.connect(txDS)


        connDS
          .keyBy(a => a.txId, b => b.txId)


          .process(new MyCoProcessFunction)
          .print()

        // 4.执行
        env.execute()
    }

    class MyCoProcessFunction extends CoProcessFunction[OrderEvent, TxEvent, String] {

        var orderMap = new mutable.HashMap[String, OrderEvent]()
        var txMap = new mutable.HashMap[String, TxEvent]()

        override def processElement1(value: OrderEvent, ctx: CoProcessFunction[OrderEvent, TxEvent, String]#Context, out: Collector[String]): Unit = {
            val maybeTxEvent: Option[TxEvent] = txMap.get(value.txId)
            if (maybeTxEvent isEmpty) {
                orderMap.put(value.txId, value)
            }else {
                out.collect("订单[" + value.txId + "]对账成功")
               txMap.remove(value.txId)
            }
        }

        override def processElement2(value: TxEvent, ctx: CoProcessFunction[OrderEvent, TxEvent, String]#Context, out: Collector[String]): Unit = {
            val maybeOrderEvent: Option[OrderEvent] = orderMap.get(value.txId)
            if (maybeOrderEvent isEmpty) {
                txMap.put(value.txId, value)
            }else {
                out.collect("订单[" + value.txId + "]对账成功")
                orderMap.remove(value.txId)
            }
        }


    }

    /**
     * 订单系统数据的样例类
     * @param orderId   订单id
     * @param eventType 行为类型：下单，支付
     * @param txId      交易码: 用来唯一标识某一笔交易，用来与交易系统连接
     * @param eventTime 事件时间：数据产生时间
     */
    case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

    /**
     * 交易系统数据的样例类
     * @param txId        交易码
     * @param payChannel  支付渠道：微信、支付宝
     * @param eventTime   时间时间：产生数据的时间
     */
    case class TxEvent(txId: String, payChannel: String, eventTime: Long)
}
