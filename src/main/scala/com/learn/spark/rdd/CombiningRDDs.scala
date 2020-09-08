package com.learn.spark.rdd

import com.learn.spark.BaseSpark
import org.apache.spark.{SparkConf, SparkContext, SparkException}

import scala.collection.mutable.ListBuffer

/**
 * @author chudichen
 * @date 2020-09-08
 */
object CombiningRDDs extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CombiningRDDs").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // 将数据放入RDD
    val letters = sc.parallelize('a' to 'z', 8)
    // 另一个相同类型的RDD
    val vowels = sc.parallelize(Seq('a', 'e', 'i', 'o', 'u'), 4)

    // 其中一个减去另一个，生成新的RDD
    val consonants = letters.subtract(vowels)
    println("There are " + consonants.count() + " consonants")

    val vowelsNotLetters = vowels.subtract(letters)
    println("There are " + vowelsNotLetters.count() + " vowels that aren't letters")

    // union
    val lettersAgain = consonants ++ vowels
    println("There really are " + lettersAgain.count() + " letters")

    // union with duplicates, removed
    val tooManyVowels = vowels ++ vowels
    println("There arent' really " + tooManyVowels.count() + " vowels")
    val justVowels = tooManyVowels.distinct()
    println("There are actually " + justVowels.count() + " vowels")

    // 减去重复
    val what = tooManyVowels.subtract(vowels)
    println("There are actually " + what.count() + " whats")

    // 交集
    val earlyLetters = sc.parallelize('a' to 'l', 2)
    val earlyVowels = earlyLetters.intersection(vowels)
    println("The early vowels:")
    earlyVowels.foreach(println)

    // 不同类型的RDD
    val numbers = sc.parallelize(1 to 2, 2)

    // 笛卡尔集
    val cp = vowels.cartesian(numbers)
    println("Product has " + cp.count() + " elements")

    // 字母增加索引
    val indexed = letters.zipWithIndex()
    println("Indexed letters")
    indexed foreach{
      case (c,i ) => println(i + " : " + c)
    }

    // 另外一个RDD
    val twentySix = sc.parallelize(101 to 126, 8)
    val differentlyIndexed = letters.zip(twentySix)
    differentlyIndexed foreach {
      case (c, i) => println(i + " : " + c)
    }

    // 如果我们两个RDD的partition不同则不可以进行合并
    // 这使得RDD的交流成本非常昂贵，我们不得不修复分区
    val twentySixBadPart = sc.parallelize(101 to 126, 3)
    val canGet = letters.zip(twentySixBadPart)
    try {
      canGet foreach {
        case (c, i) => println(i + " : " + c)
      }
    } catch {
      case e: IllegalArgumentException => println("Exception caught: " + e.getMessage)
    }

    // RDD压缩也需要相同数目的元素
    val unequalCount = earlyLetters.zip(numbers)
    try {
      unequalCount foreach {
        case (c, i) => println(i + " : " + c)
      }
    } catch {
      case se: SparkException =>
        println("Exception caught : " + se.getMessage)
    }

    /*
     * 可以使用自定义方法来处理partition不匹配的问题
     */
    def zipFunc(lIter: Iterator[Char], nIter: Iterator[Int]) : Iterator[(Char, Int)] = {
      val res = new ListBuffer[(Char, Int)]
      while (lIter.hasNext || nIter.hasNext) {
        if (lIter.hasNext && nIter.hasNext) {
          // easy case
          res += ((lIter.next(), nIter.next()))
        } else if (lIter.hasNext) {
          res += ((lIter.next(), 0))
        } else if (nIter.hasNext) {
          res += ((' ', nIter.next()))
        }
      }
      res.iterator
    }

    val unequalOK = earlyLetters.zipPartitions(numbers)(zipFunc)
    println("this may not be what you expected with unequal length RDDs")
    unequalOK foreach {
      case (c, i) => println(i + " : " + c)
    }
  }
}
