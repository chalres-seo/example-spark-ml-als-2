package com.example.config

import java.io.File
import java.util.Properties

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._

object RdbConfig extends LazyLogging {
  private val rdbConf = ConfigFactory.parseFile(new File("conf/rdb.conf")).resolve()
  logger.info("rdb config:\n\t" + rdbConf.entrySet().asScala.mkString("\n\t"))

  lazy val connectionProp: Properties = this.createConnectionProp

  val driverClass: String = rdbConf.getString("jdbc-driver-class")
  val url: String = rdbConf.getString("jdbc-url")
  val maxConnection: Int = rdbConf.getInt("max-connection")
  val user: String = rdbConf.getString("user")
  val password: String = rdbConf.getString("password")

  private def createConnectionProp = {
    val connectionProp = new Properties

    connectionProp.setProperty("driver", driverClass)
    connectionProp.setProperty("user", user)
    connectionProp.setProperty("password", password)

    connectionProp
  }
}
