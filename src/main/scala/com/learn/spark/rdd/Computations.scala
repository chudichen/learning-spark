package com.learn.spark.rdd

import com.learn.spark.BaseSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 计算以及检查点
 *
 * @author chudichen
 * @since 2020-09-08
 */
object Computations extends BaseSpark {

  private def showDep[T](r: RDD[T], depth: Int): Unit = {
    println("".padTo(depth, ' ') + "RDD id=" + r.id)
    r.dependencies.foreach(dep => {
      showDep(dep.rdd, depth + 1)
    })
  }

  private def showDep[T](r: RDD[T]): Unit = {
    showDep(r, 0)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Computations").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // 建立简单的计算
    val numbers = sc.parallelize(1 to 10, 4)
    val bigger = numbers.map(n => n * 100)
    val biggerStill = bigger.map(_ + 1)

    println("Debug string for the RDD 'biggerStill'")
    println(biggerStill.toDebugString)

    val s = biggerStill.reduce(_ + _)

    println("sum = " + s)

    println("IDs of the various RDDs")
    println("numbers: id=" + numbers.id)
    println("bigger: id=" + bigger.id)
    println("biggerStill: id=" + biggerStill.id)
    println("dependencies working back from RDD 'biggerStill'")
    showDep(biggerStill)

    val moreNumbers = bigger ++ biggerStill
    println("The RDD 'moreNumbers' has more complex dependencies")
    println(moreNumbers.toDebugString)
    println("moreNumbers: id=" + moreNumbers.id)
    showDep(moreNumbers)

    moreNumbers.cache()
    // 缓存中的数据可能会丢失，因为依赖树不会被抛弃
    println("cached it: the dependencies don't change")
    println(moreNumbers.toDebugString)
    println(moreNumbers)

    println("has RDD 'moreNumbers' been checkpointed? : " + moreNumbers.isCheckpointed)
    // 设置moreNumbers的检查点
    sc.setCheckpointDir("/tmp/sparkcps")
    moreNumbers.checkpoint()

    // 它只发生在我们强制计算的时候
    println("Now has it been checkpointed? : " + moreNumbers.isCheckpointed)
    moreNumbers.count()
    println("Now has it been checkpointed? : " + moreNumbers.isCheckpointed)
    println(moreNumbers.toDebugString)
    showDep(moreNumbers)

    // 再次，如果必要否则绝不进行计算
    println("this shouldn't throw an exception")
    val thisWillBlowUp = numbers.map {
      case (7) => throw new Exception
      case (n) => n
    }

    // 注意即使有7，也依然会执行完成
    println("the exception should get thrown now")
    try {
      println(thisWillBlowUp.count())
    } catch {
      case (e: Exception) => println("Yep, it blew up now")
    }
  }
}
