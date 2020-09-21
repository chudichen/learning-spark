package com.learn.spark.sql

import java.sql.Date

import com.learn.spark.BaseSpark
import org.apache.spark.sql.SparkSession

/**
 * 使用Spark SQL的能力来处理复杂类型的数据，
 * 如原始的DataFrame以及case class组成的序列。
 *
 * @author chudichen
 * @since 2020-09-21
 */
object ComplexTypes extends BaseSpark {

  case class Cust(id: Integer, name: String, trans: Seq[Transaction],
                  billing:Address, shipping:Address)

  case class Transaction(id: Integer, date: Date, amount: Double)

  case class Address(state:String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SQL-ComplexTypes")
      .master("local[4]")
      .getOrCreate()

    val custs = Seq(
      Cust(1, "Widget Co",
        Seq(Transaction(1, Date.valueOf("2012-01-01"), 100.0)),
        Address("AZ"), Address("CA")),
      Cust(2, "Acme Widgets",
        Seq(Transaction(2, Date.valueOf("2014-06-15"), 200.0),
          Transaction(3, Date.valueOf("2014-07-01"), 50.0)),
        Address("CA"), null),
      Cust(3, "Widgetry",
        Seq(Transaction(4, Date.valueOf("2014-01-01"), 150.0)),
        Address("CA"), Address("CA"))
    )
    import spark.implicits._
    // 将RDD转为DataFrame
    val customerDF = spark.sparkContext.parallelize(custs, 4).toDF()

    println("*** inferred schema takes nesting and arrays into account")
    customerDF.printSchema()
    customerDF.createOrReplaceTempView("customer")

    println("*** Query results reflect complex structure")
    spark.sql("SELECT * FROM customer").show()

    /*
     * 使用Spark SQL，优雅的处理复杂嵌套对象
     */
    println("*** Projecting from deep structure doesn't blow up when it's missing")
    val projectedCust = spark.sql(
      """
        | SELECT id, name, shipping.state, trans[1].date as secondTrans
        | FROM customer
        |""".stripMargin)
    projectedCust.show()
    projectedCust.printSchema()

    /*
     * 'trans'字段是一个array，但是你可以通过trans.date来遍历每一个元素
     */
    println("*** Reach into each element of an array of structures by omitting the subscript")
    val arrayOfStruct = spark.sql("SELECT id, trans.date AS transDates FROM customer")
    arrayOfStruct.printSchema()
    arrayOfStruct.show()

    println("*** Group by a nested field")
    val groupByNested = spark.sql(
      """
        | SELECT shipping.state, count(*) AS customers
        | FROM customer
        | GROUP BY shipping.state
        |""".stripMargin)
    groupByNested.show()

    println("*** Order by a nested field")
    val orderByNested = spark.sql(
      """
        | SELECT id, shipping.state
        | FROM customer
        | ORDER BY shipping.state
        |""".stripMargin)
    orderByNested.show()
  }
}
