package com.learn.spark

import org.apache.log4j.{Level, Logger}

/**
 * 去掉多余的日志信息
 */
trait BaseSpark {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
}