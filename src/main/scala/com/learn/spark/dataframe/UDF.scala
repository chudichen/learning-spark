package com.learn.spark.dataframe

import com.learn.spark.BaseSpark
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
 * User Define Function
 *
 * @author chudichen
 * @since 2020-09-11
 */
object UDF extends BaseSpark{

  private case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame-UDF")
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

    // 使用udf来通过字段创建新的字段
    val mySales = udf {(sales: Double, disc: Double) => sales - disc}
    println("*** UDF used for selecting")
    customerDF.select($"id", mySales($"sales", $"discount").as("After Discount")).show()

    // UDF filter
    val myNameFilter = udf {(s: String) => s.startsWith("W")}
    customerDF.filter(myNameFilter($"name")).show()

    // UDF grouping
    val stateRegion = udf {
      (state: String) => state match {
        case "CA" | "AK" | "OR" | "WA" => "West"
        case "ME" | "NH" | "MA" | "RI" | "CT" | "VT" => "NorthEast"
        case "AZ" | "NM" | "CO" | "UT" => "SouthWest"
      }
    }

    println("*** UDF used for grouping")
    customerDF.groupBy(stateRegion($"state").as("Region")).count().show()

    val stateFilter = udf {
      (state: String, regionState: mutable.WrappedArray[String]) =>
        regionState.contains(state)
    }

    println("*** UDF with array constant parameter")
    customerDF.filter(stateFilter($"state",
      array(lit("CA"), lit("MA"), lit("NY"), lit("NJ"))))
      .show()

    val multipleFilter = udf { (state: String, discount: Double,
                                thestruct: Row) =>
      state == thestruct.getString(0) && discount > thestruct.getDouble(1) }

    println("*** UDF with array constant parameter")
    customerDF.filter(
      multipleFilter($"state", $"discount", struct(lit("CA"), lit(100.0)))
    ).show()
  }
}
