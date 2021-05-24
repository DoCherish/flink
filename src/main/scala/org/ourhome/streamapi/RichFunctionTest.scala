package org.ourhome.streamapi

import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @Author Do
 * @Date 2020/4/19 18:13
 */
object RichFunctionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream: DataStream[String] = env.readTextFile("D:\\Work\\Code\\flinkdev\\src\\main\\resources\\textfile\\customdata.txt")
    env.setParallelism(5)

    inputStream.map(new MyRichMapFunction).print("name")

    env.execute("RichMapFunction test")
  }

  class MyRichMapFunction extends RichMapFunction[String, String] {
    var count: Int = 0
    var startTime: Long = _
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS")

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      startTime = System.currentTimeMillis()
      println("open函数调用时间：" + timeFormat.format(startTime))
      println("--------------------------------------------")
    }

    override def map(value: String): String = {
      println("map函数调用时间：" + timeFormat.format(System.currentTimeMillis()))
      count += 1
      value.split(",")(0)
    }

    override def close(): Unit = {
      println("--------------------------------------------")
      println("close函数调用时间：" + timeFormat.format(System.currentTimeMillis()))
      super.close()
      println("共统计个数：" + count)
    }
  }

}
