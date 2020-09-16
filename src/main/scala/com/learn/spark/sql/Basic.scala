package com.learn.spark.sql

import com.learn.spark.BaseSpark
import org.apache.spark.sql.SparkSession

/**
 * 通过case class来定义data，并转为DataFrame，
 * 通过注册为临时表，来查询，打印出原始的DataFrame
 * 以及查询后结果集的结构。
 *
 * @author chudichen
 * @since 2020-09-08
 */
object Basic extends BaseSpark {

  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SQL-Basic")
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

    println("*** See the DataFrame contents")
    customerDF.show()

    println("*** See the first few lines of the DataFrame contents")
    customerDF.show(2)

    println("*** Statistics for the numerical columns")
    customerDF.describe("sales", "discount").show()

    println("*** A DataFrame has a schema")
    customerDF.printSchema()

    /*
     * 注册为临时表进行查询
     */
    customerDF.createOrReplaceTempView("customer")

    println("*** Very simple query")
    val allCust = spark.sql("SELECT id, name FROM customer")
    allCust.show()
    println("*** The result has a schema too")
    allCust.printSchema()

    /*
     * 复杂查询:通过多行
     */
    println("*** Very simple query with a filter")
    val californiaCust =
      spark.sql(
        s"""
          |  SELECT id, name, sales
          |  FROM customer
          |  WHERE state = 'CA'
        """.stripMargin
      )
    californiaCust.show()
    californiaCust.printSchema()

    println("*** Queries are case sensitive by default, but this can be disabled")
    spark.conf.set("spark.sql.caseSensitive", "false")
    val caseInsensitive =
      spark.sql("SELECT * FROM CUSTOMER")
    caseInsensitive.show()
  }
}
