package org.ourhome.streamapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Author Do
 * @Date 2020/5/3 20:35
 */
object SideOutputTest2 {
  lazy val webTerminal: OutputTag[MdMsg] = new OutputTag[MdMsg]("Web端埋点数据")
  lazy val mobileTerminal: OutputTag[MdMsg] = new OutputTag[MdMsg]("移动端埋点数据")
  lazy val csTerminal: OutputTag[MdMsg] = new OutputTag[MdMsg]("CS端埋点数据")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)

    val socketData: DataStream[String] = env.socketTextStream("192.168.237.128", 9999)
    socketData.print("input data")

    val outputStream: DataStream[MdMsg] = socketData.map(line => {
      val str: Array[String] = line.split(",")
      MdMsg(str(0), str(1), str(2).toLong)
    })
      .process(new MdSplitProcessFunction)

    // Web端埋点数据流
    outputStream.getSideOutput(webTerminal).print("web")
    // Mobile端埋点数据流
    outputStream.getSideOutput(mobileTerminal).print("mobile")
    // CS端埋点数据流
    outputStream.getSideOutput(csTerminal).print("cs")

    env.execute()

  }

  case class MdMsg(mdType:String, url:String, Time:Long)

  class MdSplitProcessFunction extends ProcessFunction[MdMsg, MdMsg] {

    override def processElement(value: MdMsg, ctx: ProcessFunction[MdMsg, MdMsg]#Context, out: Collector[MdMsg]): Unit = {
      // web
      if (value.mdType == "web") {
        ctx.output(webTerminal, value)
        // mobile
      } else if (value.mdType == "mobile") {
        ctx.output(mobileTerminal, value)
        // cs
      } else if (value.mdType == "cs") {
        ctx.output(csTerminal, value)
        // others
      } else {
        out.collect(value)
      }

    }
  }

}
