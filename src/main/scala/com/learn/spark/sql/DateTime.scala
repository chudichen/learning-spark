package com.learn.spark.sql

import java.sql.{Date, Timestamp}

import com.learn.spark.BaseSpark
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StructField, StructType, TimestampType}

/**
 * 创建包含date和timestamp的DataFrame，并使用范围查询
 *
 * @author chudichen
 * @since 2020-09-16
 */
object DateTime extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SQL-DateTime")
      .master("local[4]")
      .getOrCreate()

    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("dt", DateType),
        StructField("ts", TimestampType)
      )
    )
    val rows = spark.sparkContext.parallelize(
      Seq(
        Row(
          1,
          Date.valueOf("2000-01-11"),
          Timestamp.valueOf("2011-10-02 09:48:05.123456")
        ),
        Row(
          1,
          Date.valueOf("2004-04-14"),
          Timestamp.valueOf("2011-10-02 12:30:00.123456")
        ),
        Row(
          1,
          Date.valueOf("2008-12-31"),
          Timestamp.valueOf("2011-10-02 15:00:00.123456")
        )
      ), 4
    )
    val tdf = spark.createDataFrame(rows, schema)

    tdf.printSchema()
    tdf.createOrReplaceTempView("dates_times")

    println("*** Here's the whole table")
    spark.sql("SELECT * FROM dates_times").show()

    println("*** Query for a date range")
    spark.sql(
      s"""
         | SELECT * FROM dates_times
         | WHERE dt > cast('2004-04-14' as date)
         |""".stripMargin
    ).show()

    println("*** Query to skip a timestamp range")
    spark.sql(
      s"""
         | SELECT * FROM dates_times
         | WHERE ts < cast('2011-10-02 12:30:00' as timestamp)
         |""".stripMargin
    ).show()
  }
}
