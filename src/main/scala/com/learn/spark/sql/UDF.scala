package com.learn.spark.sql

import com.learn.spark.BaseSpark
import org.apache.spark.sql.SparkSession

/**
 * 在查询中使用UDF函数
 *
 * @author chudichen
 * @since 2020-09-21
 */
object UDF extends BaseSpark {

  case class Cust(id: Integer, name: String, sales: Double, discounts: Double, state: String)

  case class SalesDisc(sales: Double, discounts: Double)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SQL-UDF")
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
    val customerTable = spark.sparkContext.parallelize(custs, 4).toDF()

    // DSL 用法——查询使用UDF但是不用SQL
    def westernState(state: String) = Seq("CA", "OR", "WA", "AK").contains(state)

    // 使用SQL用法，我们需要注册临时表
    customerTable.createOrReplaceTempView("customerTable")

    // WHERE 条件
    spark.udf.register("westernState", westernState _)
    println("UDF in a WHERE")
    val westernStates = spark.sql("SELECT * FROM customerTable WHERE westernState(state)")
    westernStates.show()

    // HAVING 条件
    def manyCustomers(cnt: Long) = cnt > 2
    spark.udf.register("manyCustomers", manyCustomers _)
    println("UDF in a HAVING")
    val statesManyCustomers = spark.sql(
      """
        | SELECT state, COUNT(id) AS custCount
        | FROM customerTable
        | GROUP BY state
        | HAVING manyCustomers(custCount)
        |""".stripMargin)
    statesManyCustomers.show()

    // GROUP BY 条件
    def stateRegion(state: String) = state match {
      case "CA" | "AK" | "OR" | "WA" => "West"
      case "ME" | "NH" | "MA" | "RI" | "CT" | "VT" => "NorthEast"
      case "AZ" | "NM" | "CO" | "UT" => "SouthWest"
    }
    spark.udf.register("stateRegion", stateRegion _)
    println("UDF in a GROUP BY")
    val salesByRegion = spark.sql(
      """
        | SELECT SUM(sales), stateRegion(state) AS totalSales
        | FROM customerTable
        | GROUP BY stateRegion(state)
        |""".stripMargin)
    salesByRegion.show()

    // 我们也可以使用UDF作用于结果字段
    def discountRatio(sales: Double, discount: Double): Double = discount / sales
    spark.udf.register("discountRatio", discountRatio _)
    println("UDF in a result")
    val customerDiscounts = spark.sql(
      """
        | SELECT id, discountRatio(sales, discounts) AS ratio
        | FROM customerTable
        |""".stripMargin)
    customerDiscounts.show()

    def makeStruct(sales: Double, disc: Double) = SalesDisc(sales, disc)
    spark.udf.register("makeStruct", makeStruct _)

    println("UDF with nested query creating structured result")
    val nestedStruct = spark.sql("SELECT id, sd.sales FROM (SELECT id, makeStruct(sales, discounts) AS sd FROM customerTable) AS d")
    nestedStruct.show()
  }
}
