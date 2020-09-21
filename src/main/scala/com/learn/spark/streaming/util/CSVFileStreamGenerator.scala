package com.learn.spark.streaming.util

import java.io.{File, PrintWriter}

import scala.util.Random


/**
 * @author chudichen
 * @since 2020-09-21
 */
class CSVFileStreamGenerator(nFiles: Int, nRecords: Int, betweenFilesMsec: Int) {

  private val root = new File(File.separator + "tmp" + File.separator + "streamFiles")
  makeExist(root)

  private val prep = new File(root.getAbsolutePath + File.separator + "prep")
  makeExist(prep)

  val dest = new File(root.getAbsoluteFile + File.separator + "desk")
  makeExist(dest)

  private def makeExist(dir: File): Unit = {
    dir.mkdir()
  }

  private def writeOutput(f: File): Unit = {
    val p = new PrintWriter(f)
    try {
      for (i <- 1 to nRecords) {
        p.println(s"Key_$i,${Random.nextInt}")
      }
    } finally {
      p.close()
    }
  }

  def makeFiles(): Unit = {
    for (n <- 1 to nFiles) {
      val f = File.createTempFile("Spark_", ".txt", prep)
      writeOutput(f)
      val nf = new File(dest + File.separator + f.getName)
      f renameTo nf
      nf.deleteOnExit()
      Thread.sleep(betweenFilesMsec)
    }
  }
}
