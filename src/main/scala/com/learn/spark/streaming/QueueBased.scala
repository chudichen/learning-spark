package com.learn.spark.streaming

import com.learn.spark.BaseSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author chudichen
 * @since 2020-09-21
 */
object QueueBased extends BaseSpark {

  class QueueMaker(sc: SparkContext, ssc: StreamingContext) {

    private val rddQueue = new mutable.Queue[RDD[Int]]()

    val inputStream = ssc.queueStream(rddQueue)

    private var base = 1

    private def makeRDD(): RDD[Int] = {
      val rdd = sc.parallelize(base to base + 94, 4)
      base = base + 100
      rdd
    }

    def populateQueue(): Unit = {
      for (n <- 1 to 10) {
        rddQueue.enqueue(makeRDD())
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("QueueBasedStreaming").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // steams 将会每秒钟处理一次
    val ssc = new StreamingContext(sc, Seconds(1))
    val qm = new QueueMaker(sc, ssc)

    // 创建流
    val stream = qm.inputStream

    // 注册的数据
    stream.foreachRDD(r => {
      println(r.count())
    })

    // 开启流
    ssc.start()

    new Thread("Streaming Termination Monitor") {
      override def run(): Unit = {
        try {
          ssc.awaitTermination()
        } catch {
          case e: Exception =>
            println("*** streaming exception ")
        }
      }
    }.start()

    println("*** started termination monitor")
    println("*** producing data")
    // 开始处理数据
    qm.populateQueue()
    Thread.sleep(15000)
    println("*** stopping data")
    ssc.stop()

    Thread.sleep(5000)
    println("*** done")
  }
}
