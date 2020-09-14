package com.learn.spark.dataset

import com.learn.spark.BaseSpark
import org.apache.spark.sql.SparkSession

/**
 * @author chudichen
 * @since 2020-09-08
 */
object Basic extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame-Basic")
      .master("local[4]")
      .getOrCreate()

    val s = Seq(10, 11, 12 ,13, 14, 15)
    import spark.implicits._
    val ds = s.toDS()

    println("*** only one column, and it always has the same name")
    ds.columns.foreach(println)

    println("*** column types")
    ds.dtypes.foreach(println)

    println("*** schema as if it was DataFrame")
    ds.printSchema()

    println("*** values > 12")
    ds.where($"value" > 12).show()

    val s2 = Seq.range(1, 100)

    println("*** size of the range")
    println(s2.size)

    val tuples = Seq(
      (1, "one", "un"),
      (2, "two", "deux"),
      (3, "three", "trois")
    )

    val tupleDS = tuples.toDS()

    println("*** Tuple Dataset types")
    tupleDS.dtypes.foreach(println)

    // 虽然dataset的column不友好，但是依然不影响你用来查询
    println("*** filter by one column and fetch another")
    tupleDS.where($"_1" > 2).select($"_2", $"_3").show()
  }
}
