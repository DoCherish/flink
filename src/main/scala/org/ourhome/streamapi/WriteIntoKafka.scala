package org.ourhome.streamapi

import java.util.Properties

import org.apache.flink.api.common.serialization.{SimpleStringSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumerBase, FlinkKafkaProducer}

/**
 * @Author Do
 * @Date 2020/4/15 23:22
 */
object WriteIntoKafka {
  private val KAFKA_TOPIC: String = "kafka_producer_test"

  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val runType:String = params.get("runtype")
    println("runType: " + runType)

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "ip:host")
    properties.setProperty("group.id", "kafka_consumer")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // exactly-once 语义保证整个应用内端到端的数据一致性
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 开启检查点并指定检查点时间间隔为5s
    env.enableCheckpointing(5000) // checkpoint every 5000 msecs
    // 设置StateBackend，并指定状态数据存储位置
    env.setStateBackend(new FsStateBackend("file:///D:/Temp/checkpoint/flink/KafkaSource"))

    val dataSource: FlinkKafkaConsumerBase[String] = new FlinkKafkaConsumer(
      KAFKA_TOPIC,
      new SimpleStringSchema(),
      properties)
      .setStartFromLatest()  // 指定从最新offset开始消费

    val dataStream: DataStream[String] = env.addSource(dataSource)
    val kafkaSink: FlinkKafkaProducer[String] = new FlinkKafkaProducer[String](
      "brokerList",
      "topic",
      new SimpleStringSchema()
    )
    dataStream.addSink(kafkaSink)

    // execute program
    env.execute("Flink Streaming—————KafkaSource and KafkaSink")
  }
}
