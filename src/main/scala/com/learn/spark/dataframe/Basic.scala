package com.learn.spark.dataframe

import com.learn.spark.BaseSpark
import org.apache.spark.sql.SparkSession

/**
 * 通过RDD创建DataFrame，并执行一些DataFrame操作，
 * DataFame可以通过RDD[Row]或者Schema创建
 *
 * @author chudichen
 * @date 2020-09-08
 */
object Basic extends BaseSpark{

  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame-Basic")
      .master("local[4]")
      .getOrCreate()


    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )

    // 将RDD转为DataFrame
    import spark.implicits._
    val customerDF = spark.sparkContext.parallelize(custs, 4).toDF()

    println("*** toString() just gives you the schema")

    println(customerDF.toString())

    println("*** It's better to use printSchema()")

    customerDF.printSchema()

    println("*** show() gives you neatly formatted data")

    customerDF.show()

    println("*** use select() to choose one column")

    customerDF.select("id").show()

    println("*** use select() for multiple columns")

    customerDF.select("sales", "state").show()

    println("*** use filter() to choose rows")

    customerDF.filter($"state".equalTo("CA")).show()
  }
}
