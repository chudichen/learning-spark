package com.learn.spark.streaming

import com.learn.spark.BaseSpark
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.learn.spark.streaming.util.CSVFileStreamGenerator

/**
 * 通过文件创建Streaming
 *
 * @author chudichen
 * @since 2020-09-21
 */
object FileBased extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FileBasedStreaming").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // 流将会每秒处理一次文件
    val ssc = new StreamingContext(sc, Seconds(1))
    val fm = new CSVFileStreamGenerator(10, 100, 500)

    // 创建流
    val stream = ssc.textFileStream(fm.dest.getAbsolutePath)

    stream.foreachRDD(r => println(r.count()))

    // 启动流
    ssc.start()

    new Thread("Streaming Termination Monitor") {
      override def run() {
        try {
          ssc.awaitTermination()
        } catch {
          case e: Exception =>
            println("*** streaming exception caught in monitor thread")
            e.printStackTrace()
        }
        println("*** streaming terminated")
      }
    }.start()

    println("*** started termination monitor")
    Thread.sleep(2000)
    println("*** producing data")
    fm.makeFiles()

    Thread.sleep(10000)
    println("*** stopping streaming")
    ssc.stop()

    // 等待终止返回
    Thread.sleep(5000)

    println("*** done")
  }
}
