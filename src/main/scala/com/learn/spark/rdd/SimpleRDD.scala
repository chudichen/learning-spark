package com.learn.spark.rdd

import com.learn.spark.BaseSpark
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD的Hello world
 *
 * @author chudichen
 * @date 2020-09-08
 */
object SimpleRDD extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SimpleRDD").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // 将数据放入RDD中
    val numbers = 1 to 10
    val numbersRDD = sc.parallelize(numbers, 4)
    println("Print each element of original RDD")
    numbersRDD.foreach(println)

    // 对数据进行简单操作
    val stillAnRDD = numbersRDD.map(n => n.toDouble / 2)

    // 获取数据输出结果
    val nowAnArray = stillAnRDD.collect()
    // 有趣的是为什么输出的结果进行排序了，而RDD却没有
    println("Now print each element of the transformed array")
    nowAnArray.foreach(println)

    // 查看RDD属性
    val partitions = stillAnRDD.glom()
    println("we should have 4 partitions")
    println(partitions.count())
    partitions.foreach(a => {
      println("Partition contents: " + a.foldLeft("")((s, e) => s + " " + e))
    })
  }
}
