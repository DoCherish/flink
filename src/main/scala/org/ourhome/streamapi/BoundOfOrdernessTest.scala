package org.ourhome.streamapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

/**
 * @Author Do
 * @Date 2020/4/25 9:25
 */
object BoundOfOrdernessTest {
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
      .assignTimestampsAndWatermarks(new MyAssigner)
        //.peocess(...)

    env.execute()
  }

  class MyAssigner extends BoundedOutOfOrdernessTimestampExtractor[(String, Int)](Time.seconds(3)) {
    override def extractTimestamp(element: (String, Int)): Long = {
      element._2
    }
  }

}
