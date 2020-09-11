package com.learn.spark.dataframe

import com.learn.spark.BaseSpark
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructType}

/**
 * 用户自定义聚合函数
 *
 * @author chudichen
 * @date 2020-09-11
 */
object UDAF extends BaseSpark {


  /**
   * 统计售价超过$500的UDAF函数
   */
  private class ScalaAggregateFunction extends UserDefinedAggregateFunction {

    /**
     * 输入很多个参数，只取一个字段
     *
     * @return 加入sales
     */
    override def inputSchema: StructType = new StructType().add("sales", DoubleType)

    /**
     * 一个聚合函数可以有多个值，这里只需要一个
     *
     * @return 总和
     */
    override def bufferSchema: StructType = new StructType().add("sumLargeSales", DoubleType)

    /**
     * 返回一个Double，总和
     *
     * @return 总和
     */
    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    /**
     * 每个部门的总和处理值为0
     *
     * @param buffer 初始值
     */
    override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0, 0.0)

    /**
     * 如果超$500则累加
     *
     * @param buffer 累加
     * @param input 输入值
     */
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val sum = buffer.getDouble(0)
      if (!input.isNullAt(0)) {
        val sales = input.getDouble(0)
        if (sales > 500.0) {
          buffer.update(0, sum + sales)
        }
      }
    }

    /**
     * 将所有分区数量累加
     *
     * @param buffer1 分区1
     * @param buffer2 分区2
     */
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0))
    }

    /**
     * 聚合结果只有一个值
     *
     * @param buffer 累加器
     * @return 聚合结果
     */
    override def evaluate(buffer: Row): Any = buffer.getDouble(0)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame-UDAF")
      .master("local[4]")
      .getOrCreate()

    val custs = Seq(
      (1, "Widget Co", 120000.00, 0.00, "AZ"),
      (2, "Acme Widgets", 410500.00, 500.00, "CA"),
      (3, "Widgetry", 200.00, 200.00, "CA"),
      (4, "Widgets R Us", 410500.00, 0.0, "CA"),
      (5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )

    val customerRows = spark.sparkContext.parallelize(custs, 4)
    import spark.implicits._
    val customerDF = customerRows.toDF("id", "name", "sales", "discount", "state")

    val mySum = new ScalaAggregateFunction()

    customerDF.printSchema()
    customerDF.show()

    val results = customerDF.groupBy("state").agg(mySum($"sales").as("bigsales"))

    results.printSchema()
    results.show()
  }
}
