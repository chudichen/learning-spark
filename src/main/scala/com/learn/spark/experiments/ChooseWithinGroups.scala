package com.learn.spark.experiments

import com.learn.spark.BaseSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * 从组中获取元素
 *
 * @author chudichen
 * @since 2020-09-24
 */
object ChooseWithinGroups extends BaseSpark {

  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Experiments")
      .master("local[4]")
      .getOrCreate()

    val people = Seq(
      (5, "Bob", "Jones", "Canada", 23),
      (7, "Fred", "Smith", "Canada", 18),
      (5, "Robert", "Andrews", "USA", 32)
    )
    val peopleRows = spark.sparkContext.parallelize(people, 4)
    import spark.implicits._
    val peopleDF = peopleRows.toDF("id", "first", "last", "country", "age")

    // 设置默认
    spark.conf.set("spark.sql.retainGroupColumns", "true")

    val maxAge = peopleDF.select($"id" as "mid", $"age" as "mage")
      .groupBy("mid").agg(max($"mage") as "maxage")

    val maxAgeAll = maxAge.join(peopleDF,
      maxAge("maxage") === peopleDF("age") and maxAge("mid") === peopleDF("id"),
      "inner").select("id", "first", "last", "country", "age")
    maxAgeAll.show()

    type Payload = (String, String, String, Int)

    val pairs: RDD[(Int, Payload)] = peopleRows.map({
      case (id: Int, first: String, last: String, country: String, age: Int) =>
        (id, (first, last, country, age))
    })

    def add(acc: Option[Payload], rec: Payload): Option[Payload] = {
      acc match {
        case None => Some(rec)
        case Some(previous) => if (rec._4 > previous._4) Some(rec) else acc
      }
    }

    def combine(acc1: Option[Payload], acc2: Option[Payload]): Option[Payload] = {
      (acc1, acc2) match {
        case (None, None) => None
        case (None, _) => acc2
        case (_, None) => acc1
        case (Some(p1), Some(p2)) => if (p1._4 > p2._4) acc1 else acc2
      }
    }

    val start: Option[Payload] = None

    val withMax: RDD[(Int, Option[Payload])] =
      pairs.aggregateByKey(start)(add, combine)
    withMax.collect().foreach(println)
  }
}
