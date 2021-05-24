package org.ourhome.streamapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Author Do
 * @Date 2020/4/25 21:33
 */
object EventTimeTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val socketData: DataStream[String] = env.socketTextStream("192.168.237.128", 9999)
    socketData.print("input ")

    socketData.map(line => {
      val str: Array[String] = line.split(",")
      ConsumerMess(str(0).toInt, str(1).toDouble, str(2).toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ConsumerMess](Time.seconds(2)) {
        override def extractTimestamp(element: ConsumerMess): Long = {
          element.Time
        }
      })
      .keyBy(_.userId)
      .timeWindow(Time.seconds(10))  // 滚动窗口
      .reduce((a, b) => ConsumerMess(a.userId, a.spend + b.spend, b.Time))
      .print("output ")

    env.execute()

  }

  case class ConsumerMess(userId:Int, spend:Double, Time:Long)

}
