package com.learn.spark.dataset

import com.learn.spark.BaseSpark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/**
 * 使用嵌套对象创建Dataset
 *
 * @author chudichen
 * @date 2020-09-14
 */
object ComplexType extends BaseSpark {

  // 对所有的案例
  case class Point(x: Double, y: Double)

  // 案例一
  case class Segment(from: Point, to: Point)
  // 案例二
  case class Line(name: String, points: Array[Point])
  // 案例三
  case class NamedPoints(name: String, points:Map[String, Point])
  // 案例四
  case class NameAndMaybePoint(name: String, point: Option[Point])

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Dataset-ComplexType")
      .master("local[4]")
      .getOrCreate()

    // 案例一嵌套case class
    println("*** 案例一：嵌套case class")
    val segments = Seq(
      Segment(Point(1.0, 2.0), Point(3.0, 4.0)),
      Segment(Point(8.0, 2.0), Point(3.0, 14.0)),
      Segment(Point(11.0, 2.0), Point(3.0, 24.0))
    )
    import spark.implicits._
    val segmentDS = segments.toDS()

    segmentDS.printSchema()
    println("*** filter by one column and fetch another")
    segmentDS.where($"from".getField("x") > 7.0).select($"to").show()

    // 案例二：arrays
    println("*** 案例二：arrays")
    val lines = Seq(
      Line("a", Array(Point(0.0, 0.0), Point(2.0, 4.0))),
      Line("b", Array(Point(-1.0, 0.0))),
      Line("c", Array(Point(0.0, 0.0), Point(2.0, 6.0), Point(10.0, 100.0)))
    )
    val linesDS = lines.toDS()
    linesDS.printSchema()

    println("*** filter by an array element")
    linesDS
      .where($"points".getItem(2).getField("y") > 7.0)
      .select($"name", size($"points").as("count"))
      .show()

    // 案例三： maps
    val namedPoints = Seq(
      NamedPoints("a", Map("p1" -> Point(0.0, 0.0))),
      NamedPoints("b", Map("p1" -> Point(0.0, 0.0),
        "p2" -> Point(2.0, 6.0), "p3" -> Point(10.0, 100.0)))
    )
    val namedPointsDS = namedPoints.toDS()
    namedPointsDS.printSchema()
    println("*** filter and select using map lookup")
    namedPointsDS.where(size($"points") > 1)
      .select($"name", size($"points").as("count"), $"points".getItem("p1")).show()

    // 案例四：Option
    println("*** Example 4: Option")
    val maybePoints = Seq(
      NameAndMaybePoint("p1", None),
      NameAndMaybePoint("p2", Some(Point(-3.1, 99.99))),
      NameAndMaybePoint("p3", Some(Point(1.0, 2.0))),
      NameAndMaybePoint("p4", None)
    )
    val maybePointsDS = maybePoints.toDS()
    maybePointsDS.printSchema()

    println("*** filter by nullable column resulting from Option type")
    maybePointsDS.where($"point".getField("y") > 50.0)
      .select($"name", $"point")
      .show()

    println("*** again its fine also to select through a column that's sometimes null")
    maybePointsDS.select($"point".getField("x")).show()
  }
}
