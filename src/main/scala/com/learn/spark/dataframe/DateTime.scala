package com.learn.spark.dataframe

import java.sql.{Date, Timestamp}

import com.learn.spark.BaseSpark
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
 * 针对DateType和TimestampType的别查询
 *
 * @author chudichen
 * @date 2020-09-10
 */
object DateTime extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame-DateTime")
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
          Date.valueOf("1999-01-11"),
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
      ), 4)

    val tdf = spark.createDataFrame(rows, schema)
    println("DataFrame with both DateType and TimestampType")
    tdf.show()

    import spark.implicits._
    println("Pull a DateType apart when querying")
    tdf.select($"dt", year($"dt"), quarter($"dt"), month($"dt"),
      weekofyear($"dt"), dayofyear($"dt"), dayofmonth($"dt")).show()

    println("Date arithmetic")
    tdf.select($"dt", datediff(current_date(), $"dt"),
      date_sub($"dt", 20),
      date_add($"dt", 10),
      add_months($"dt", 6)).show()

    println("Date truncation")
    tdf.select($"dt", trunc($"dt", "YYYY"),
      trunc($"dt", "YY"),
      trunc($"dt", "MM"))
      .show()

    println("Date formatting")
    tdf.select($"dt", date_format($"dt", "MM dd YYYY")).show()

    println("Pull a Timestamp type apart when querying")
    tdf.select($"ts", year($"ts"), hour($"ts"), minute($"ts"), second($"ts")).show()
  }
}
