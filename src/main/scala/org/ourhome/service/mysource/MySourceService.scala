package org.ourhome.service.mysource

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * @Author Do
 * @Date 2021/7/25 22:21
 */
object MySourceService {

  def process(env: StreamExecutionEnvironment): Unit = {

    /**自定义source*/
    val source: DataStream[(Int, String)] = env.addSource[(Int, String)](new MySource)

    /**用于产生异常情况导致程序重启*/
    val socketSource: DataStream[String] = env.socketTextStream("node01", 8888)
    socketSource.map(line => {
      if (line.startsWith("q")) {
        println(1/0)
      } else {
        println(line)
      }
    })

    source.print()

  }

}
