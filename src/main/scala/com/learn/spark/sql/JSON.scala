package com.learn.spark.sql

import com.learn.spark.BaseSpark
import org.apache.spark.sql.SparkSession

/**
 * 解析json
 *
 * @author chudichen
 * @since 2020-09-17
 */
object JSON extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SQL-JSON")
      .master("local[4]")
      .getOrCreate()

    val people = spark.read.json("src/main/resources/data/flat.json")
    people.printSchema()
    people.createOrReplaceTempView("people")
    val young = spark.sql("SELECT firstName, lastName FROM people WHERE age < 30")
    young.show()

    val peopleAddr = spark.read.json("src/main/resources/data/notFlat.json")
    peopleAddr.printSchema()
    peopleAddr.show()
    peopleAddr.createOrReplaceTempView("peopleAddr")
    val inPA = spark.sql("SELECT firstName, lastName FROm peopleAddr WHERE address.state = 'PA'")
    inPA.show()

    val peopleAddrBad = spark.read.json("src/main/resources/data/notFlatBadFieldName.json")
    peopleAddrBad.printSchema()

    val lines = spark.read.textFile("src/main/resources/data/notFlatBadFieldName.json")
    import spark.implicits._
    val linesFixed = lines.map(s => s.replaceAllLiterally("$", ""))

    linesFixed.show()
  }
}
