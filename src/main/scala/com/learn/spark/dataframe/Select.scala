package com.learn.spark.dataframe

import com.learn.spark.BaseSpark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * @author chudichen
 * @since 2020-09-10
 */
object Select extends BaseSpark {

  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame-Select")
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

    println("*** user * to select() all columns")
    customerDF.select("*").show()

    println("*** select multiple columns")
    customerDF.select("id", "discount").show()

    println("*** use apply() on DataFrame to create column objects, and select though them")
    customerDF.select(customerDF("id"), customerDF("discount")).show()

    println("*** use as() on column to rename")
    customerDF.select(customerDF("id").as("Customer ID"),
      customerDF("discount").as("Total Discount")).show()

    println("*** $ as shorthand to obtain Column")
    customerDF.select($"id".as("Customer ID"), $"discount".as("Total Discount")).show()

    println("*** user DSL to manipulate values")
    customerDF.select(($"discount" * 2).as("Double Discount")).show()
    customerDF.select(($"sales" - $"discount").as("After Discount")).show()

    println("*** use * to select() all columns and add more")
    customerDF.select($"*", $"id".as("newID")).show()

    println("*** use lit() to add a literal column")
    customerDF.select($"id", $"name", lit(42).as("FortyTwo")).show()

    println("*** use array() to combine multiple results into single array column")
    customerDF.select($"id", array($"name", $"state", lit("hello")).as("Stuff")).show()

    println("*** use rand() to add random number between 0.0 and .0 inclusive")
    customerDF.select($"id", rand().as("r")).show()
  }
}
