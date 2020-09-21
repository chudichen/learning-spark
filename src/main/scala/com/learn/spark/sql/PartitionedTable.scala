package com.learn.spark.sql

import java.io.File

import com.learn.spark.BaseSpark
import com.learn.spark.util.PartitionedTableHierarchy
import org.apache.spark.sql.SparkSession

/**
 * 使用Spark SQL 来操作分区文件
 *
 * @author chudichen
 * @since 2020-09-18
 */
object PartitionedTable extends BaseSpark {

  case class Fact(year: Integer, month: Integer, id: Integer, cat: Integer)

  def main(args: Array[String]): Unit = {
    // 设置输出路径，并清空文件
    val exampleRoot = "/tmp/LearningSpark"
    PartitionedTableHierarchy.deleteRecursively(new File(exampleRoot))
    val tableRoot = exampleRoot + "/Table"

    val spark = SparkSession.builder()
      .appName("SQL-PartitionedTable")
      .master("local[4]")
      .getOrCreate()

    val ids = 1 to 1200
    val facts = ids.map(id => {
      val month = id % 12 + 1
      val year = 2000 + (month % 12)
      val cat = id % 4
      Fact(year, month, id, cat)
    })

    import spark.implicits._
    // 转为DataFrame
    val factsDF = spark.sparkContext.parallelize(facts, 4).toDF()
    println("*** Here is some of the same data")
    factsDF.show()

    // 注册临时表
    factsDF.createOrReplaceTempView("original")
    spark.sql(
      """
        | DROP TABLE IF EXISTS partitioned
        |""".stripMargin)

    spark.sql(
      s"""
         | CREATE TABLE partitioned
         |    (year INTEGER, month INTEGER, id INTEGER, cat INTEGER)
         | USING PARQUET
         | PARTITIONED BY (year, month)
         | LOCATION '/tmp/LearningSpark'
      """.stripMargin)

    spark.sql(
      """
        | INSERT INTO TABLE partitioned
        | SELECT id, cat, year, month FROM original
        |""".stripMargin)

    println("*** partitioned table in the file system, after the initial insert")
    PartitionedTableHierarchy.printRecursively(new File(tableRoot))
  }

}
