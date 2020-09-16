package com.learn.spark.sql

import com.learn.spark.BaseSpark
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @author chudichen
 * @since 2020-09-16
 */
object Types extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SQL_Types")
      .master("local[4]")
      .getOrCreate()

    val numericRows = Seq(
      Row(1.toByte, 2.toShort, 3, 4.toLong,
        BigDecimal(1), BigDecimal(2), 3.0f, 4.0)
    )
    val numericSchema = StructType(
      Seq(
        StructField("a", ByteType),
        StructField("b", ShortType),
        StructField("c", IntegerType),
        StructField("d", LongType),
        StructField("e", DecimalType(10, 5)),
        StructField("f", DecimalType(20, 10)),
        StructField("g", FloatType),
        StructField("h", DoubleType)
      )
    )
    val numericRowsRDD = spark.sparkContext.parallelize(numericRows, 4)
    val numericDF = spark.createDataFrame(numericRowsRDD, numericSchema)

    numericDF.printSchema()
    numericDF.show()
    numericDF.createOrReplaceTempView("numeric")

    spark.sql("SELECT * FROM numeric").show()
  }
}
