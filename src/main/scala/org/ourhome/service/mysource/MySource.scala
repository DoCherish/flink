package org.ourhome.service.mysource

import java.io.RandomAccessFile
import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

/**
 * @Author Do
 * @Date 2021/7/25 22:38
 */
class MySource extends RichParallelSourceFunction[(Int, String)] with CheckpointedFunction {

  private val readPath: String = "src/main/resources/textfile/"
  private val modeType: String = "r"

  private var offset: Long = 0L
  private var flag: Boolean = true

  private var offsetState: ListState[Long] = _

  override def run(ctx: SourceFunction.SourceContext[(Int, String)]): Unit = {
    /** 先取状态数据 */
    val offsetStateIterator: util.Iterator[Long] = offsetState.get().iterator()
    while (offsetStateIterator.hasNext) {
      offset = offsetStateIterator.next()
    }


    /** 当前subtask的索引 */
    val indexOfSubtask: Int = getRuntimeContext.getIndexOfThisSubtask

    if (indexOfSubtask == 0 || indexOfSubtask == 1) {

      /** 读取的文件 */
      val accessFile = new RandomAccessFile(readPath + indexOfSubtask + ".txt", modeType)
      accessFile.seek(offset)

      /** 获取一个锁 */
      val clock = ctx.getCheckpointLock()

      while (flag) {
        val readLine: String = accessFile.readLine()

        if (readLine != null) {
          val line = new String(readLine.getBytes("ISO-8859-1"), "utf-8")

          /** 更新offset时加锁，没有更新完不能执行snapshotState */
          synchronized {
            offset = accessFile.getFilePointer()
            ctx.collect((indexOfSubtask, line))
          }

        } else {
          Thread.sleep(3 * 1000)
        }

      }

    }


  }

  override def cancel(): Unit = {
    flag = false

  }

  /**
   * 定期将状态数据保存到StateBackend
   *
   * @param context FunctionInitializationContext
   */
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    /** 将历史值清空 */
    offsetState.clear()

    /** 更新最新的状态值 */
    offsetState.add(offset)
  }

  /**
   * 初始化Operator State
   * 生命周期方法，在run方法执行前执行一次
   *
   * @param context FunctionInitializationContext
   */
  override def initializeState(context: FunctionInitializationContext): Unit = {

    /** 定义一个状态描述器 */
    val stateDescriptor: ListStateDescriptor[Long] = new ListStateDescriptor[Long]("offset-state", classOf[Long])

    /** 初始化状态，或获取历史状态 */
    offsetState = context.getOperatorStateStore.getListState(stateDescriptor)


  }
}
