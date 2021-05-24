package org.ourhome.streamapi

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Author Do
 * @Date 2020/5/4 22:36
 */
object StateCalculationTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val socketData: DataStream[String] = env.socketTextStream("192.168.237.128", 9999)
    socketData.print("input data")
    socketData.map(line => {
      val strings: Array[String] = line.split(",")
      (strings(0), strings(1), strings(2).toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, String, Long)](Time.seconds(2)) {
        override def extractTimestamp(element: (String, String, Long)): Long = {
          element._3
        }
      })
      .keyBy(0)
      .timeWindow(Time.seconds(60))
//      .aggregate()


    env.execute()
  }
}
