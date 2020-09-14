package com.learn.spark.dataframe

import com.learn.spark.BaseSpark
import com.learn.spark.dataframe.GroupingAndAggregation.Cust
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/**
 * 使用UDF函数
 *
 * @author chudichen
 * @since 2020-09-10
 */
object Transform extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame-Transform")
      .master("local[4]")
      .getOrCreate()

    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )

    import spark.implicits._
    val customerDF = spark.sparkContext.parallelize(custs, 4).toDF()

    // 原始DataFrame
    customerDF.show()

    val myFunc = udf {(x: Double) => x + 1}

    val colNames = customerDF.columns
    val cols = colNames.map(cName => customerDF.col(cName))
    val theColumn = customerDF("discount")
    val mappedCols = cols.map(c => if (c.toString() == theColumn.toString()) myFunc(c).as("transformed") else c)

    val newDF = customerDF.select(mappedCols:_*)
    newDF.show()
  }
}
