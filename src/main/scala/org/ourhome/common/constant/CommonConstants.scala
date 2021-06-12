package org.ourhome.common.constant

/**
 * @Author Do
 * @Date 2021/5/25 10:06
 */
object CommonConstants {

  // 环境及配置文件
  var ENV: String = _
  val ENV_KEY: String = "env"
  val DEFAULT_ENV: String = "test"
  val PROPERTIES_PATH: String = "/env/config." + ENV + ".properties"

  val STREAM_PARALLELISM: String = "stream.parallelism"
  val STREAM_RESTART_ATTEMPT: String = "stream.restart.attempt"
  val STREAM_RESTART_DELAY_BETWEEN_ATTEMPTS_INTERVAL: String = "stream.restart.delay.between.attempts.interval"

}
