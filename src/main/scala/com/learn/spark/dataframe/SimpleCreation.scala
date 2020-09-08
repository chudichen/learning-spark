package com.learn.spark.dataframe

import com.learn.spark.BaseSpark
import org.apache.spark.sql.SparkSession

/**
 * @author chudichen
 * @date 2020-09-08
 */
object SimpleCreation extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame-SimpleCreation")
      .master("local[4]")
      .getOrCreate()

    val custs = Seq(
      (1, "Widget Co", 120000.00, 0.00, "AZ"),
      (2, "Acme Widgets", 410500.00, 500.00, "CA"),
      (3, "Widgetry", 410500.00, 200.00, "CA"),
      (4, "Widgets R Us", 410500.00, 0.0, "CA"),
      (5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )

    // 将tuples 转为RDD
    val customerRows = spark.sparkContext.parallelize(custs, 4)

    // 将RDD 转为DataFrame
    import spark.implicits._
    val customerDF = customerRows.toDF()

    customerDF.printSchema()
    customerDF.show()
  }
}
