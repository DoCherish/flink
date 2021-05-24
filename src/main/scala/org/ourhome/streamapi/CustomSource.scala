package org.ourhome.streamapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.util.Random

/**
 * @Author Do
 * @Date 2020/4/15 22:04
 */
object CustomSource {
  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val runType:String = params.get("runtype")
    println("runType: " + runType)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // exactly-once 语义保证整个应用内端到端的数据一致性
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 开启检查点并指定检查点时间间隔为5s
    env.enableCheckpointing(5000) // checkpoint every 5000 msecs
    // 设置StateBackend，并指定状态数据存储位置
    env.setStateBackend(new FsStateBackend("file:///D:/Temp/checkpoint/flink/KafkaSource"))
    // 使用自定义SourceFunction
    val dataSource: DataStream[List[Double]] = env.addSource(new CustomGenerator())
    dataSource.print().setParallelism(1)

    env.execute("Custom Source")
  }

  // 自定义Source，重写SourceFunction两个方法
  class CustomGenerator extends SourceFunction[List[Double]] {
    private var running = true
    override def run(ctx: SourceFunction.SourceContext[List[Double]]): Unit = {
      // 随机数生成器
      var randomNum: Random = new Random()

      while (running) {
        val n = 1.to(5).map(i => {
          i + randomNum.nextGaussian()
        }).toList
        // 利用ctx上下文将数据返回
        ctx.collect(n)

        Thread.sleep(500)
      }

    }

    override def cancel(): Unit = {
      running = false
    }
  }

}
