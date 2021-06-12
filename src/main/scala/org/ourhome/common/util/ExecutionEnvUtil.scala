package org.ourhome.common.util

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.ourhome.common.constant.CommonConstants._

/**
 * @Author Do
 * @Date 2021/5/25 9:59
 */

object InitEnvUtil {

  @throws[Exception]
  def createParameterTool(args: Array[String]): ParameterTool = {
    val fromArgs: ParameterTool = ParameterTool.fromArgs(args)  // Example arguments: --key1 value1 --key2 value2 -key3 value3
    val fromSystem: ParameterTool = ParameterTool.fromSystemProperties()

    ENV = fromArgs.get(ENV_KEY, DEFAULT_ENV)
    println(s"env: $ENV")

    ParameterTool.fromPropertiesFile(this.getClass.getResourceAsStream(PROPERTIES_PATH))
      .mergeWith(fromArgs)
      .mergeWith(fromSystem)

  }

  def prepare(parameterTool: ParameterTool): StreamExecutionEnvironment = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parameterTool.getInt(STREAM_PARALLELISM, 5))
    env.getConfig.disableSysoutLogging()

    val restartAttempts: Int = parameterTool.getInt(STREAM_RESTART_ATTEMPT, 4)
    val delayBetweenAttempts: Int = parameterTool.getInt(STREAM_RESTART_DELAY_BETWEEN_ATTEMPTS_INTERVAL, 60000)

    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts, delayBetweenAttempts))

    env.getConfig.setGlobalJobParameters(parameterTool)

    env

  }


}
