package com.example.config

import java.io.File

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._

object AppConfig extends LazyLogging {
  private val appConf = ConfigFactory.parseFile(new File("conf/app.conf")).resolve()
  logger.debug("application config:\n\t" + appConf.entrySet().asScala.mkString("\n\t"))

  def getLimitNumPartition: Int = this.appConf.getInt("limit-number-partition")
}
