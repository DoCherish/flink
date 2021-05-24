package org.ourhome.streamapi

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Author Do
 * @Date 2020/4/19 21:28
 */
object Redis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val inputStream: DataStream[String] = env.readTextFile("D:\\Work\\Code\\flinkdev\\src\\main\\resources\\textfile\\windowdata.txt")
    inputStream.map(line => (line.split(",")(0).toInt, line.split(",")(1).toInt)).keyBy(_._1).print("input")
      inputStream.map(line => (line.split(",")(0).toInt, line.split(",")(1).toInt)).keyBy(_._1)
      .timeWindow(Time.milliseconds(100), Time.milliseconds(50))
        .aggregate(new AggregateFunction[(Int, Int), (Int, Int), Double] {
          override def createAccumulator(): (Int, Int) = (0, 0)

          override def add(value: (Int, Int), accumulator: (Int, Int)): (Int, Int) = {
            (accumulator._1 + 1, accumulator._2 + value._2)
          }

          override def getResult(accumulator: (Int, Int)): Double = {
            println("666666")
            accumulator._2/accumulator._1
          }

          override def merge(a: (Int, Int), b: (Int, Int)): (Int, Int) = {
            (a._1 + b._1, a._2 + b._2)
          }
        }).print("output:")

    env.execute()
  }
}
