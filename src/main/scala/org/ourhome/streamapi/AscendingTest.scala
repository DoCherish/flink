package org.ourhome.streamapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Author Do
 * @Date 2020/4/25 9:14
 */
object AscendingTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val socketData: DataStream[String] = env.socketTextStream("192.168.237.128", 9999)
    // 使用Ascending生成时间戳和watermark
    socketData.map(line => {
      (line.split(",")(0), line.split(",")(1).toInt)
    })
      // 指定时间字段
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .timeWindow(Time.minutes(3))
      .sum(0)
      .print()

    env.execute()
  }

}
