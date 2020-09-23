package com.learn.spark.streaming

import com.learn.spark.BaseSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._
import scala.util.Success

/**
 * 简单的自定义Receiver。每秒100ms发送一条string，
 * 在自定义Receiver中依次执行操作。
 *
 * @author chudichen
 * @since 2020-09-23
 */
class CustomReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) with Serializable {

  /**
   * 两种和接受者通信的线程：一个promise/future来要求停止，或者是
   * 查看何时停止。在 onStart()时进行初始化。
   */
  private class Synchronization {
    val promiseToTerminate = Promise[Unit]()
    val futureTermination = promiseToTerminate.future

    val promiseToStop = Promise[Unit]()
    val futureStopping = promiseToStop.future
  }

  private var sync: Synchronization = null

  /**
   * 开启很容易：接受者线程调用onStart()方法即可
   */
  override def onStart(): Unit = {
    println("*** starting custom receiver")
    sync = new Synchronization
    new Thread("Custom Receiver") {
      override def run(): Unit = {
        receive()
      }
    }
  }

  /**
   * 停止稍微有一些复杂：一旦调用了onStop()方法，接受者将会被
   * 判定为停止状态，因此你需要通知所有线程，以及等待他们的结束
   * 最好也设置一个过期时间。
   */
  override def onStop(): Unit = {
    println("*** stopping custom receiver")
    sync.promiseToTerminate.complete(Success())
    Await.result(sync.futureStopping, 1 second)
    println("*** stopped custom receiver")
  }

  /**
   * 就终止而言，这个循环接收有些棘手，首先如果'isStopped'设置为true时，
   * 应该被终止，因为streaming认为接受者已经停止了，但是这还不够，循环也应该被结束掉。
   */
  private def receive(): Unit = {
    while (!isStarted() && !sync.futureTermination.isCompleted) {
      try {
        // 短暂停留一下
        Thread.sleep(100)
        store("Hello")
      } catch {
        case e: Exception =>
          println("*** exception caught in receiver calling store()")
          e.printStackTrace()
      }
    }
    sync.promiseToStop.complete(Success())
    println("*** custom receiver loop exited")
  }
}

object CustomStreaming extends BaseSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSeqStreaming").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // 每秒钟处理
    val ssc = new StreamingContext(sc, Seconds(1))

    val stream = ssc.receiverStream(new CustomReceiver)

    stream.foreachRDD(r => {
      println(s"Items: ${r.count()} Partitions: ${r.partitions.length}")
    })

    println("*** starting streaming")
    ssc.start()

    new Thread("Delayed Termination") {
      override def run(): Unit = {
        Thread.sleep(15000)
        println("*** stopping streaming")
        ssc.stop()
      }
    }.start()

    try {
      ssc.awaitTermination()
      println("*** streaming terminated")
    } catch {
      case e: Exception => println("*** streaming exception caught in monitor thread")
    }
    Thread.sleep(5000)
  }
}