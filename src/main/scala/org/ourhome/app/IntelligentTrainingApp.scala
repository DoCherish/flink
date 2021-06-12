package org.ourhome.app

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.ourhome.common.util.InitEnvUtil._

/**
 * @Author Do
 * @Date 2021/5/25 9:43
 */
object IntelligentTrainingApp {

  def main(args: Array[String]): Unit = {
    val parameterTool: ParameterTool = createParameterTool(args)
    val env: StreamExecutionEnvironment = prepare(parameterTool)

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
//    env.setStateBackend(new MemoryStateBackend())



    env.execute("test_flink")
  }



}
