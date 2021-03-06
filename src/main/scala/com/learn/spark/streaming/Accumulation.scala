package com.learn.spark.streaming

import com.learn.spark.BaseSpark
import com.learn.spark.streaming.QueueBased.QueueMaker
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 演示简单的通过分批每次累加数据值，这次是通过
 * 设置全局变量，这种做法很常用，但并不是最有效
 * 以及最好的方法。
 *
 * @author chudichen
 * @since 2020-09-21
 */
object Accumulation extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Accumulation").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    var acc = sc.parallelize(Seq(0), 4)

    val qm = new QueueMaker(sc, ssc)
    val stream = qm.inputStream
    stream.foreachRDD(r => {
      acc = acc ++ r
      println("Count in accumulator: " + acc.count)
      println("Batch size: " + r.count())
    })
    ssc.start()

    new Thread("Delayed Termination") {
      override def run(): Unit = {
        qm.populateQueue()
        Thread.sleep(15000)
        println("*** stopping streaming")
        ssc.stop()
      }
    }.start()

    try {
      ssc.awaitTermination()
      println("*** streaming terminated")
    } catch {
      case e: Exception => {
        println("*** streaming exception caught in monitor thread")
      }
    }

    println("*** done")
  }
}
