package org.ourhome.streamapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @Author Do
 * @Date 2020/4/18 21:19
 */
object RedisSinkTest {
  private val REDIS_KEY = "person_message"

  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val runType: String = params.get("runtype")
    println("runType: " + runType)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.enableCheckpointing(5000)
    env.setStateBackend(new FsStateBackend("file:///D:/Work/Code/flinkdev/src/main/resources/checkpoint"))

    val inputStream: DataStream[String] = env.readTextFile("D:\\Work\\Code\\flinkdev\\src\\main\\resources\\textfile\\customdata.txt")
    // 处理inputStream，包装成Person类
    val streaming: DataStream[Person] = inputStream.map(line => {
      println(line)
      val strings: Array[String] = line.split(",")
      Person(strings(0).trim, strings(1).trim.toInt, strings(2).trim, strings(3).trim.toFloat)
    })
    // 配置redis conf
    val redisConf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("host")
      .setPort(6379)
      .setTimeout(30000)
      .build()
    streaming.addSink(new RedisSink[Person](redisConf, new MyRedisMapper))

    env.execute("Redis Sink")
  }

  case class Person(name: String, age: Int, gender: String, height: Float)

  //自定义MyRedisMapper
  class MyRedisMapper extends RedisMapper[Person] {
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET, REDIS_KEY)
    }

    // redis filed
    override def getKeyFromData(t: Person): String = {
      t.name
    }

    // redis value
    override def getValueFromData(t: Person): String = {
      t.age + ":" + t.gender + ":" + t.height
    }

  }

}
