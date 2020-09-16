package com.learn.spark.dataset

import java.io.File

import com.learn.spark.BaseSpark
import com.learn.spark.util.PartitionedTableHierarchy
import org.apache.spark.sql.SparkSession

/**
 * 一组操作大量数据的练习，创建出一个代表整个数据结构的目录,
 * 将数据分片保存在层级目录的叶子节点中。通常的做法是以天为
 * 单位，然后层级目录结构分别是以年，月，日。此外也有以位置
 * 和分类为层级的结构。这里也可以合并在一个层级中。
 *
 * 这个练习中的方法非常普遍，以至于已经继承到了Spark和Hadoop
 * 的特性中，更加方便的创建层级目录和查询。比较常见的做法是
 * 创建Parquet文件，但是为了演示方便看得到目录结构就创建为
 * csv文件。
 *
 * 这个案例中将一系列的交易记录以年/月为层级保存在文件中。这
 * 样的创建问题是输出文件太多以至于超出了你的预期，有时候数量
 * 非常庞大。这样就会使你难以整理。增加你的存储成本，降低接下
 * 来的查询效率。问题的关键点在于如果有效的切分并生成文件。
 * 如果DataSet在没有经过字段分离的情况下，创建文件，这会使得
 * 在创建时带有额外的分区信息，从而创建大量的文件。
 *
 * 使用partitionBy()但并未使用repartition()将数据写入分区
 * 将使问题变得更糟。在你将数据集输出到文件之前可以使用repartition()
 * 来控制文件数量。
 *
 * @author chudichen
 * @since 2020-09-14
 */
object PartitionBy extends BaseSpark {

  case class Transaction(id: Long, year: Int, month: Int, day: Int,
                         quantity: Long, price: Double)

  def main(args: Array[String]): Unit = {
    val exampleRoot = "/tmp/LearningSpark"
    val spark = SparkSession.builder()
      .appName("Dataset-PartitionBy")
      .master("local[4]")
      .config("spark.default.parallelism", 12)
      .getOrCreate()

    PartitionedTableHierarchy.deleteRecursively(new File(exampleRoot))

    val transactions = Seq(
      // 2016-11-05
      Transaction(1001, 2016, 11, 5, 100, 42.99),
      Transaction(1002, 2016, 11, 5, 75, 42.99),
      // 2016-11-15
      Transaction(1003, 2016, 11, 15, 50, 75.95),
      Transaction(1004, 2016, 11, 15, 50, 19.95),
      Transaction(1005, 2016, 11, 15, 25, 42.99),
      // 2016-12-11
      Transaction(1006, 2016, 12, 11, 22, 11.00),
      Transaction(1007, 2016, 12, 11, 100, 170.00),
      Transaction(1008, 2016, 12, 11, 50, 5.99),
      Transaction(1009, 2016, 12, 11, 10, 11.00),
      // 2016-12-22
      Transaction(1010, 2016, 12, 22, 20, 10.99),
      Transaction(1011, 2016, 12, 22, 10, 75.95),
      // 2017-01-01
      Transaction(1012, 2017, 1, 2, 1020, 9.99),
      Transaction(1013, 2017, 1, 2, 100, 19.99),
      // 2017-01-31
      Transaction(1014, 2017, 1, 31, 200, 99.95),
      Transaction(1015, 2017, 1, 31, 80, 75.95),
      Transaction(1016, 2017, 1, 31, 200, 100.95),
      // 2017-02-01
      Transaction(1017, 2017, 2, 1, 15, 22.00),
      Transaction(1018, 2017, 2, 1, 100, 75.95),
      Transaction(1019, 2017, 2, 1, 5, 22.00),
      // 2017-02-22
      Transaction(1020, 2017, 2, 22, 5, 42.99),
      Transaction(1021, 2017, 2, 22, 100, 42.99),
      Transaction(1022, 2017, 2, 22, 75, 11.99),
      Transaction(1023, 2017, 2, 22, 50, 42.99),
      Transaction(1024, 2017, 2, 22, 200, 99.95)
    )

    import spark.implicits._
    val transactionsDS = transactions.toDS()

    // 默认的并行数量
    println("*** number of partitions: " + transactionsDS.rdd.partitions.length)

    /*
     * 首先将数据保存在CSV文件中，不使用任何的目录层级，
     * 这就相当于使用一个文件来保存所有的数据，可能这个
     * 文件会相当巨大，以至于必须要使用一个集群来存储，
     * 但是读取十分便利。
     */
    val simpleRoot = exampleRoot + "/Simple"
    transactionsDS.write
      .option("header", "true")
      .csv(simpleRoot)

    println("*** Simple output file count: " +
      PartitionedTableHierarchy.countRecursively(new File(simpleRoot), ".csv"))
    PartitionedTableHierarchy. printRecursively(new File(simpleRoot))

    /*
     * 这次我们使用年/月的层级目录来存储，这次我们会得到
     * 非常多的文件，因为相同的月份有可能存在于不同的partition
     * 上，如果数据量很大，或者partition很多的情况下，
     * 这时的文件数量可能非常惊人。
     *
     * 注意：最坏的结果是你所有的分区数量乘以区分的字段数量
     */
    val partitionedRoot = exampleRoot + "/Partitioned"
    transactionsDS.write
      .partitionBy("year", "month")
      .option("header", "true")
      .csv(partitionedRoot)

    println("*** Date partitioned output file count: " + PartitionedTableHierarchy.countRecursively(new File(partitionedRoot), ".csv"))
    PartitionedTableHierarchy.printRecursively(new File(partitionedRoot))

    /*
     * 这里在输出之前我们首先使用repartition。你有很大的
     * 空间来控制如何重建DataSet，当数据量非常大的时候这样
     * 做是很有必要的，基本的思想是，你想让每个partition的
     * 叶子节点上有多少个文件夹。这里不必只有一个，我们可以使
     * 用年/月来进行分层。
     */
    val repartitionedRoot = exampleRoot + "/Repartitioned"
    transactionsDS.repartition($"year", $"month").write
      .partitionBy("year", "month")
      .option("header", "true")
      .csv(repartitionedRoot)
    println("*** Date repartitioned output file count: " +
      PartitionedTableHierarchy.countRecursively(new File(repartitionedRoot), ".csv"))
    PartitionedTableHierarchy.printRecursively(new File(repartitionedRoot))

    val allDF = spark.read
      .option("basePath", partitionedRoot)
      .option("header", "true")
      .csv(partitionedRoot)
    allDF.show()

    val oneDF = spark.read
      .option("basePath", partitionedRoot + "/year=2016/month=11")
      .option("header", "true")
      .csv(partitionedRoot + "/year=2016/month=11")

    oneDF.show()

    val oneMonth = spark.read
      .option("basePath", partitionedRoot)
      .option("header", "true")
      .csv(partitionedRoot + "/year=2016/month=11")
    oneMonth.show()

    val twoMonthQuery = spark.read
      .option("basePath", partitionedRoot)
      .option("header", "true")
      .csv(partitionedRoot)
      .filter("year = 2016")
    twoMonthQuery.show()

  }
}
