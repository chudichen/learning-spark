package com.learn.spark.streaming

import com.learn.spark.BaseSpark
import com.learn.spark.streaming.QueueBased.QueueMaker
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 滑动窗口
 *
 * @author chudichen
 * @since 2020-09-23
 */
object Windowing extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Windowing").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // 每秒钟处理一次
    val ssc = new StreamingContext(sc, Seconds(1))
    val qm = new QueueMaker(sc, ssc)

    // 创建流
    val stream = qm.inputStream

    // 注册数据，每2秒统计前5秒的数据
    stream.window(Seconds(5), Seconds(2)).foreachRDD(r => {
      if (r.count() == 0) {
        println("Empty")
      } else {
        println("Count = " + r.count() + " min = " + r.min() + " max = " + r.max())
      }
    })

    // streaming启动
    ssc.start()

    new Thread("Delayed Termination") {
      override def run(): Unit = {
        qm.populateQueue()
        Thread.sleep(20000)
        println("*** stopping streaming")
        ssc.stop()
      }
    }.start()

    try {
      ssc.awaitTermination()
      println("*** streaming terminated")
    } catch {
      case exception: Exception => println("*** streaming exception caught in monitor thread")
    }
  }
}
