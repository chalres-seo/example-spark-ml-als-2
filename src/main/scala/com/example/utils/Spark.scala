package com.example.utils

import java.util.Properties

import com.databricks.spark.avro._
import com.example.config.{AppConfig, RdbConfig, SparkConfig}
import com.typesafe.scalalogging.LazyLogging
import org.apache
import org.apache.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by charles on 11/23/17.
  */
object Spark extends LazyLogging {
  private val defaultSparkConfiguration = SparkConfig.sparkConfiguration

  val sparkSession: SparkSession = SparkSession.builder()
    .config(defaultSparkConfiguration)
    .enableHiveSupport()
    .getOrCreate()

  logger.info(s"spark version : ${sparkSession.version}")

//  private val rdbJdbcURL = RdbConfig.url
//  private val rdbMaxConn = RdbConfig.maxConnection
//  private val rdbConnectionProperties = RdbConfig.connectionProp



//  def saveToRDB(df:DataFrame, db:String, table:String, mode:String): Unit = {
//    logger.info(s"write df to rdb")
//    logger.info(s"mode : $mode")
//    logger.info(s"db : $db")
//    logger.info(s"table : $table")
//
//    df.coalesce(rdbMaxConn)
//      .write
//      .mode(mode)
//      .jdbc(rdbJdbcURL, s"${db.toUpperCase}.dbo.${table.toUpperCase}", rdbConnectionProperties)
//
//  }
//
//  def writeToAvro(df: DataFrame, country:String, path:String, mode:String): Unit = {
//    df.withColumn("country", lit(country))
//      //.coalesce()
//      .write
//      .mode(mode)
//      .avro(path)
//  }
//
//  def writeToAvro(df: DataFrame, path:String, mode:String): Unit = {
//    df
//      //.coalesce(Checker.checkNumPartition(df))
//      .write
//      .mode(mode)
//      .avro(path)
//  }
}
