package com.learn.spark.dataset

import com.learn.spark.BaseSpark
import org.apache.spark.sql.SparkSession

/**
 * 使用case class来生成dataset
 *
 * @author chudichen
 * @date 2020-09-11
 */
object CaseClass extends BaseSpark{

  case class Number(i: Int, english: String, french: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Dataset-CaseClass")
      .master("local[4]")
      .getOrCreate()

    val numbers = Seq(
      Number(1, "one", "un"),
      Number(2, "two", "deux"),
      Number(3, "three", "trois")
    )
    import spark.implicits._
    val numberDS = numbers.toDS()
    println("*** case class Dataset types")
    numberDS.dtypes.foreach(println)

    println("*** filter by one column and fetch another")
    numberDS.where($"i" > 2).select($"english", $"french").show()

    println("*** could have used SparkSession.createDataset() instead")
    val anotherDS = spark.createDataset(numbers)

    println("*** case class Dataset types")
    anotherDS.dtypes.foreach(println)
  }
}
