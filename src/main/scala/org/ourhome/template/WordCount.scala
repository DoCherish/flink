package org.ourhome.template

/**
 * @Author Do
 * @Date 2020/4/12 23:08
 */
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {

    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")
    // set env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // add data
    val socketData: DataStream[String] = env.socketTextStream(host, port)
    // transformation
    val wordCountData = socketData.flatMap(_.toLowerCase.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    // add sink or print
    wordCountData.print()
    // name flink job and trigger task
    env.execute("wordcount")

  }
}
