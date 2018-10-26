package com.example.config

import java.io.File
import java.util.Map

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

object SparkConfig extends LazyLogging {
  private val sparkConf = ConfigFactory.parseFile(new File("conf/spark.conf")).resolve()
  logger.info("spark config:\n\t" + sparkConf.entrySet().asScala.mkString("\n\t"))

  private val master: String = this.sparkConf.getString("master")
  private val appName: String = this.sparkConf.getString("app-name")
  private val warehouseDir: String = this.sparkConf.getString("warehouse-dir")
  private val hiveMetastoreUris: String = this.sparkConf.getString("hive-metastore-uris")

  val hdfsRootPath: String = this.sparkConf.getString("hdfs-path-root")

  lazy val sparkConfiguration: SparkConf = this.createSparkConfiguration

  private def createSparkConfiguration = {
    val sparkConfiguration = new SparkConf()

    sparkConfiguration
      .setMaster(master)
      .setAppName(appName)
      .set("spark.warehouse.dir", warehouseDir)
      .set("hive.metastore.uris", hiveMetastoreUris)

    sparkConfiguration
  }
}
