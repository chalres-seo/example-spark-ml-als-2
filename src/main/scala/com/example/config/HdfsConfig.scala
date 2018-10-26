package com.example.config

import java.io.File

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.joda.time.DateTime

import scala.collection.JavaConverters._

object HdfsConfig extends LazyLogging {
  private val hdfsConf = ConfigFactory.parseFile(new File("conf/hdfs.conf")).resolve()
  logger.info("hdfs config:\n\t" + hdfsConf.entrySet().asScala.mkString("\n\t"))

  private val hdfsPathConf = hdfsConf.getConfig("path").resolve()
  logger.info("hdfs path config:\n\t" + hdfsPathConf.entrySet().asScala.mkString("\n\t"))

  private val hdfsSourceConf = hdfsConf.getConfig("path.source").resolve()
  logger.info("hdfs source path config:\n\t" + hdfsSourceConf.entrySet().asScala.mkString("\n\t"))

  val uri: String = hdfsConf.getString("uri")
  val defaultFS: String = hdfsConf.getString("fs.defaultFS")
  val replicationFactor: Int = hdfsConf.getInt("dfs.replication")
  val user: String = hdfsConf.getString("user")
  val rootPath: String = hdfsPathConf.getString("root")
  val mlPath: String = hdfsPathConf.getString("ml")
  val sourceList: Vector[String] = hdfsSourceConf.entrySet().asScala.map(_.getKey).filter(!_.contains("root")).toVector

  lazy val hdfsConfigurtion: Configuration = this.createHdfsConfiguration

  private def createHdfsConfiguration: Configuration = {
    val hdfsConfiguration = new Configuration()

    hdfsConfiguration.set("fs.defaultFS", defaultFS)
    hdfsConfiguration.set("dfs.replication", replicationFactor.toString)

    hdfsConfiguration
  }

  def getHdfsSourcePath(sourceName: String): String = hdfsSourceConf.getString(sourceName)

  def getHdfsSourceYearPath(sourceName: String, country:String, year: Int): String = {
    s"${this.getHdfsSourcePath(sourceName)}/" +
      s"country=$country/" +
      s"year=$year"
  }

  def getHdfsSourceMonthPath(sourceName: String, country:String, year: Int, month: Int): String = {
    s"${this.getHdfsSourcePath(sourceName)}/" +
      s"country=$country/" +
      s"year=$year/" +
      s"month=$month"
  }

  def getHdfsSourceDayPath(sourceName: String, country:String, year: Int, month: Int, day: Int): String = {
    s"${this.getHdfsSourcePath(sourceName)}/" +
      s"country=$country/" +
      s"year=$year/" +
      s"month=$month/" +
      s"day=$day"
  }

  def getHdfsSourceDatePath(sourceName: String, country:String, dateTime: DateTime): String = {
    s"${this.getHdfsSourcePath(sourceName)}" +
      s"/country=$country/" +
      s"year=${dateTime.year()}/" +
      s"month=${dateTime.monthOfYear()}/" +
      s"day=${dateTime.dayOfMonth()}"
  }



}