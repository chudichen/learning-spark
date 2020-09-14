package com.learn.spark.dataset

import java.io.File

import com.learn.spark.BaseSpark
import com.learn.spark.util.PartitionedTableHierarchy
import org.apache.spark.sql.SparkSession

/**
 * @author chudichen
 * @since 2020-09-14
 */
object PartitionBy extends BaseSpark {

  case class Transaction(id: Long, year: Int, month: Int, day: Int,
                         quantity: Long, price: Double)

  def main(args: Array[String]): Unit = {
    val exampleRoot = "/tmp/LearningSpark"
    val spark = SparkSession.builder()
      .appName("Dataset-PartitionBy")
      .master("local[4]")
      .config("spark.default.parallelism", 12)
      .getOrCreate()

    PartitionedTableHierarchy.deleteRecursively(new File(exampleRoot))

    val transactions = Seq(
      // 2016-11-05
      Transaction(1001, 2016, 11, 5, 100, 42.99),
      Transaction(1002, 2016, 11, 5, 75, 42.99),
      // 2016-11-15
      Transaction(1003, 2016, 11, 15, 50, 75.95),
      Transaction(1004, 2016, 11, 15, 50, 19.95),
      Transaction(1005, 2016, 11, 15, 25, 42.99),
      // 2016-12-11
      Transaction(1006, 2016, 12, 11, 22, 11.00),
      Transaction(1007, 2016, 12, 11, 100, 170.00),
      Transaction(1008, 2016, 12, 11, 50, 5.99),
      Transaction(1009, 2016, 12, 11, 10, 11.00),
      // 2016-12-22
      Transaction(1010, 2016, 12, 22, 20, 10.99),
      Transaction(1011, 2016, 12, 22, 10, 75.95),
      // 2017-01-01
      Transaction(1012, 2017, 1, 2, 1020, 9.99),
      Transaction(1013, 2017, 1, 2, 100, 19.99),
      // 2017-01-31
      Transaction(1014, 2017, 1, 31, 200, 99.95),
      Transaction(1015, 2017, 1, 31, 80, 75.95),
      Transaction(1016, 2017, 1, 31, 200, 100.95),
      // 2017-02-01
      Transaction(1017, 2017, 2, 1, 15, 22.00),
      Transaction(1018, 2017, 2, 1, 100, 75.95),
      Transaction(1019, 2017, 2, 1, 5, 22.00),
      // 2017-02-22
      Transaction(1020, 2017, 2, 22, 5, 42.99),
      Transaction(1021, 2017, 2, 22, 100, 42.99),
      Transaction(1022, 2017, 2, 22, 75, 11.99),
      Transaction(1023, 2017, 2, 22, 50, 42.99),
      Transaction(1024, 2017, 2, 22, 200, 99.95)
    )

    import spark.implicits._
    val transactionsDS = transactions.toDS()

    println("*** number of partitions: " + transactionsDS.rdd.partitions.size)
  }
}
