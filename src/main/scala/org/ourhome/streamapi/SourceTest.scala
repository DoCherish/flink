package org.ourhome.streamapi

import org.apache.flink.streaming.api.scala._

/**
 * @author Do
 * @Date 2020/4/13 21:08
 */
object SourceTest {
  private val DATA_FILE_PATH: String = "D:\\Work\\Code\\flinkdev\\src\\main\\resources\\textfile\\data.txt"

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream1: DataStream[String] = env.readTextFile(DATA_FILE_PATH)
//    val dataStream2: DataStream[String] = env.socketTextStream("", 9999)
    val dataStream3: DataStream[Long] = env.generateSequence(1, 9)

    dataStream3.print().setParallelism(1)

    env.execute("source test")
  }
}
