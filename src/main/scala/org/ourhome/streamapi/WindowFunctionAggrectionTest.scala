package org.ourhome.streamapi

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Author Do
 * @Date 2020/4/24 22:51
 */
object WindowFunctionAggrectionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)

    val socketData: DataStream[String] = env.socketTextStream("192.168.237.128", 9999)
    socketData.print("input ")

    socketData.map(line => {
        ConsumerMess(line.split(",")(0).toInt, line.split(",")(1).toDouble)
      })
      .keyBy(_.userId)
      .timeWindow(Time.seconds(30))
      .aggregate(new MyAggregrateFunction)
        .print("output ")

    env.execute()

  }

  case class ConsumerMess(userId:Int, spend:Double)

  //<IN>  The type of the values that are aggregated (input values)
  //<ACC> The type of the accumulator (intermediate aggregate state).
  //<OUT> The type of the aggregated result
  class MyAggregrateFunction extends AggregateFunction[ConsumerMess, (Int, Double), Double] {
    override def createAccumulator(): (Int, Double) = (0, 0)

    override def add(value: ConsumerMess, accumulator: (Int, Double)): (Int, Double) = {
      (accumulator._1 + 1, accumulator._2 + value.spend)
    }

    override def getResult(accumulator: (Int, Double)): Double = {
      accumulator._2/accumulator._1
    }

    override def merge(a: (Int, Double), b: (Int, Double)): (Int, Double) = {
      (a._1 + b._1, b._2 + a._2)
    }
  }

}
