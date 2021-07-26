package org.ourhome.service.mysource

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @Author Do
 * @Date 2021/7/25 22:21
 */
object MySourceService {

  def process(env: StreamExecutionEnvironment): Unit = {

    import org.apache.flink.api.scala._
    val source: DataStream[(Int, String)] = env.addSource[(Int, String)](new MySource)

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
