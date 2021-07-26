package org.ourhome.app.mysource

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.ourhome.common.util.InitEnvUtil.{createParameterTool, prepare}
import org.ourhome.service.mysource.MySourceService._

/**
 * @Author Do
 * @Date 2021/7/25 21:59
 */
object MyExactlyOnceParaSourceApp {

  def main(args: Array[String]): Unit = {

    //    val parameterTool: ParameterTool = createParameterTool(args)
    //    val env: StreamExecutionEnvironment = prepare(parameterTool)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.enableCheckpointing(1*1000)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 3*1000))
    env.setStateBackend(new FsStateBackend("file:///D:\\Work\\Code\\flink\\src\\main\\resources\\checkpoint\\chk1"))

    process(env)


    env.execute("test_flink")

  }

}
