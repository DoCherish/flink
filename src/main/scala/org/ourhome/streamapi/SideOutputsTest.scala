package org.ourhome.streamapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Author Do
 * @Date 2020/5/3 19:51
 */
object SideOutputsTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)

    val socketData: DataStream[String] = env.socketTextStream("192.168.237.128", 9999)
    socketData.print("input data")

    socketData.map(line => {
      val str: Array[String] = line.split(",")
      RequestMess(str(0), str(1).toInt, str(2).toLong)
    })
      .process(new TimeOutAlert)
      .getSideOutput(new OutputTag[String]("Time out!"))
      .print("alert data ")


    env.execute()

  }

  case class RequestMess(interfaceId:String, spend:Int, Time:Long)

  // 如果请求超时（>500ms）
  class TimeOutAlert extends ProcessFunction[RequestMess, RequestMess] {
    lazy val alertOutput: OutputTag[String] = new OutputTag[String]("Time out!")

    override def processElement(value: RequestMess, ctx: ProcessFunction[RequestMess, RequestMess]#Context, out: Collector[RequestMess]): Unit = {
      if (value.spend > 500) {
        ctx.output(alertOutput, "Time out for " + value.interfaceId)
      } else {
        out.collect(value)
      }
    }
  }

}
