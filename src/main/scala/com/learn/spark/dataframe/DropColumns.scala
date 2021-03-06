package com.learn.spark.dataframe

import com.learn.spark.BaseSpark
import org.apache.spark.sql.SparkSession

/**
 * @author chudichen
 * @since 2020-09-10
 */
object DropColumns extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame-DropColumns")
      .master("local[4]")
      .getOrCreate()

    // 通过tuples创建RDD
    val custs = Seq(
      (1, "Widget Co", 120000.00, 0.00, "AZ"),
      (2, "Acme Widgets", 410500.00, 500.00, "CA"),
      (3, "Widgetry", 410500.00, 200.00, "CA"),
      (4, "Widgets R Us", 410500.00, 0.0, "CA"),
      (5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )

    val customerRows = spark.sparkContext.parallelize(custs, 4)

    // 通过提供名字将RDD转换为DataFrame
    import spark.implicits._
    val customerDF = customerRows.toDF("id", "name", "sales", "discount", "state")
    println("*** Here's the whole DataFrame")
    customerDF.printSchema()
    customerDF.show()

    // 删除列
    val fewerCols = customerDF.drop("sales").drop("discount")

    println("*** Now with fewer columns")
    fewerCols.printSchema()
    fewerCols.show()
  }
}
