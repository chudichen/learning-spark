package com.learn.spark.dataframe

import com.learn.spark.BaseSpark
import org.apache.spark.sql.SparkSession

/**
 * 注意：不同参数的range实现会有不同
 *
 * @author chudichen
 * @date 2020-09-10
 */
object Range extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame-Range")
      .master("local[4]")
      .getOrCreate()

    println("*** dense range with default partitioning")
    val df1 = spark.range(10, 14)
    df1.show()
    println("# Partitions = " + df1.rdd.partitions.length)

    println("\n*** stepped range")
    val df2 = spark.range(10, 14, 2)
    df2.show()
    println(df2.rdd.partitions.length)

    println("\n*** stepped range with specified partitioning")
    val df3 = spark.range(10, 14, 2, 2)
    df3.show()
    println("# Partitions = " + df3.rdd.partitions.length)

    println("\n*** range with just a limit")
    val df4 = spark.range(3)
    df4.show()
    println("# Partitions = " + df4.rdd.partitions.length)
  }
}
