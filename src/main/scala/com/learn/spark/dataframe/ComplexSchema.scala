package com.learn.spark.dataframe

import com.learn.spark.BaseSpark
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * 这里我们通过RDD[ROW]来创建一个合成的DataFrame,
 * 我们会尝试DataFrame中更多的可能性
 *
 * @author chudichen
 * @date 2020-09-10
 */
object ComplexSchema extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame-ComplexSchema")
      .master("local[4]")
      .getOrCreate()

    // 案例一：嵌套行
    val rows1 = Seq(
      Row(1, Row("a", "b"), 8.00, Row(1, 2)),
      Row(2, Row("c", "d"), 9.00, Row(3, 4))
    )
    val rows1RDD = spark.sparkContext.parallelize(rows1, 4)
    val schema1 = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("s1", StructType(
          Seq(
            StructField("x", StringType),
            StructField("y", StringType)
          )
        )),
        StructField("d", DoubleType),
        StructField("s2", StructType(
          Seq(
            StructField("u", IntegerType),
            StructField("v", IntegerType)
          )
        ))
      )
    )

    println("Position of subfield 'd' is " + schema1.fieldIndex("d"))

    import spark.implicits._
    val df1 = spark.createDataFrame(rows1RDD, schema1)

    println("Schema with nested Row")
    df1.show()

    println("Select the column with nested Row at the top level")
    df1.select("s1").show()

    println("Select deep into the column with nested Row")
    df1.select("s1.x").show()

    println("The column function getField() seems to be 'right' way")
    df1.select($"s1".getField("X")).show()

    // 案例二：ArrayType
    val rows2 = Seq(
      Row(1, Row("a", "b"), 8.00, Array(1, 2)),
      Row(2, Row("c", "d"), 9.00, Array(3, 4, 5))
    )

    val rows2RDD = spark.sparkContext.parallelize(rows2, 4)

    // 这次通过构造函数添加字段，并且通过add()方法来添加
    val schema2 = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("s1", StructType(
          Seq(
            StructField("x", StringType),
            StructField("y", StringType)
          )
        ))
      )
    ).add(StructField("d", DoubleType))
      .add("a", ArrayType(IntegerType))

    val df2 = spark.createDataFrame(rows2RDD, schema2)

    println("Schema with array")
    df2.printSchema()

    println("DataFrame with array")
    df2.show()

    println("Count elements of each array in the column")
    df2.select($"id", size($"a").as("count")).show()

    println("Explode the array elements out into additional rows")
    df2.select($"id", explode($"a").as("element")).show()

    println("Appy a membership test to each array in a column")
    df2.select($"id", array_contains($"a", 2).as("has 2")).show()

    println("Use column function getItem() to into array when selecting")
    df2.select($"id", $"a".getItem(2)).show()

    // 案例三：MapType
    val row3 = Seq(
      Row(1, 8.00, Map("u" -> 1, "v" -> 2)),
      Row(2, 9.00, Map("x" -> 3, "y" -> 4, "z" -> 5))
    )

    val rows3RDD = spark.sparkContext.parallelize(row3, 4)
    val schema3 = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("d", DoubleType),
        StructField("m", MapType(StringType, IntegerType))
      )
    )
    val df3 = spark.createDataFrame(rows3RDD, schema3)

    println("Schema with map")
    df3.printSchema()

    println("DataFrame with map")
    df3.show()

    println("Count elements of each map in the column")
    df3.select($"id", explode($"m")).show()

    // MapType是一个可伸缩的类型，其中的key可以被当做是column来搜索，
    // 如果搜不到时，则为null
    println("Select deep into the column with a Map")
    df3.select($"id", $"m.u").show()

    println("The column function getItem() seems to be the 'right' way")
    df3.select($"id", $"m".getItem("u")).show()
  }
}
