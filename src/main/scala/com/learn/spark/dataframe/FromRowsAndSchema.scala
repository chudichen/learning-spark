package com.learn.spark.dataframe

import com.learn.spark.BaseSpark
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * 通过RDD[Row]和合成的schema来创建DataFrame
 *
 * @author chudichen
 * @since 2020-09-09
 */
object FromRowsAndSchema extends BaseSpark{

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame-FromRowAndSchema")
      .master("local[4]")
      .getOrCreate()

    // 通过数据创建RDD
    val custs = Seq(
      Row(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Row(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Row(3, "Widgetry", 410500.00, 200.00, "CA"),
      Row(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Row(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    val customerRows = spark.sparkContext.parallelize(custs, 4)

    val customerSchema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("sales", DoubleType),
        StructField("discount", DoubleType),
        StructField("state", StringType)
      )
    )

    // 将来RDD[ROW]与schema合并在一起生成DataFrame
    val customerDF = spark.createDataFrame(customerRows, customerSchema)
    customerDF.printSchema()
    customerDF.show()
  }
}
