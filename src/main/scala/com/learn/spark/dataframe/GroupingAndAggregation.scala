package com.learn.spark.dataframe

import org.apache.spark.sql.functions._
import com.learn.spark.BaseSpark
import org.apache.spark.sql.{Column, SparkSession}

/**
 * 演示grouping和aggregation的用法
 *
 * @author chudichen
 * @date 2020-09-09
 */
object GroupingAndAggregation extends BaseSpark {

  case class Cust(id: Int, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame-GroupingAndAggregation")
      .master("local[4]")
      .getOrCreate()

    // 创建一组case class的Seq
    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )

    // 将RDD转换为DataFrame
    import spark.implicits._
    val customerDF = spark.sparkContext.parallelize(custs, 4).toDF()

    // 通过groupBy()来产生分组的数据
    println("*** basic form of aggregation")
    customerDF.groupBy("state").agg("discount" -> "max").show()

    println("*** this time without grouping columns")
    spark.conf.set("spark.sql.retainGroupColumn", "false")
    customerDF.groupBy("state").agg("discount" -> "max").show()

    println("*** Column based aggregation")
    customerDF.groupBy("state").agg(max($"discount")).show()

    println("*** Column based aggregation plus grouping columns")
    customerDF.groupBy("state").agg($"state", max($"discount")).show()

    def stddevFunc(c: Column): Column = sqrt(avg(c * c) - (avg(c) * avg(c)))

    println("*** Sort-of a user-defined aggregation function")
    customerDF.groupBy("state").agg($"state", stddevFunc($"discount")).show()

    println("*** Aggregation short cuts")
    customerDF.groupBy("state").count().show()
  }
}
