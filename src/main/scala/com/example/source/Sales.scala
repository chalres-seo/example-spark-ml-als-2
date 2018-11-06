package com.example.source

import java.sql.Date

import com.example.config.HdfsConfig
import com.example.utils.Spark
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.B
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object Sales extends LazyLogging {
  private val sourceFileName = "Video_Games_Sales_as_at_22_Dec_2016.csv"
  private val sourceFilePath = s"${HdfsConfig.uri}/tmp/$sourceFileName"

  private val schema = StructType(Array(
    StructField("name", StringType, nullable = true),
    StructField("platform", StringType, nullable = true),
    StructField("year_of_release", IntegerType, nullable = true),
    StructField("genre", StringType, nullable = true),
    StructField("publisher", StringType, nullable = true),
    StructField("na_sales", DoubleType, nullable = true),
    StructField("eu_sales", DoubleType, nullable = true),
    StructField("jp_sales", DoubleType, nullable = true),
    StructField("other_sales", DoubleType, nullable = true),
    StructField("global_sales", DoubleType, nullable = true),
    StructField("critic_score", IntegerType, nullable = true),
    StructField("critic_count", IntegerType, nullable = true),
    StructField("user_score", DoubleType, nullable = true),
    StructField("user_count", IntegerType, nullable = true),
    StructField("developer", StringType, nullable = true),
    StructField("rating", StringType, nullable = true)))

  private val spark = Spark.sparkSession
  import spark.implicits._

  def getRawDataSource: DataFrame = {
    spark.read
      .option("header", "true")
      .csv(sourceFilePath)

  }

  def getInferSchemaDF: DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(sourceFilePath)

  }

  def getCustomSchemaDF: DataFrame = {
    spark.read
      .option("header", "true")
      .schema(schema)
      .csv(sourceFilePath)
  }

  def getDS: Dataset[Sales] = {
    this.getCustomSchemaDF.as[Sales]
  }

//  private def rowToSales(row: Row): Sales = {
//    Sales(row.getString(0),
//      row.getString(1),
//      fillNullValue(row.getString(2))(_.toInt)(_ => 0),
//      row.getString(3),
//      row.getString(4),
//      fillNullValue(row.getString(5))(_.toDouble)(_ => 0),
//      fillNullValue(row.getString(6))(_.toDouble)(_ => 0),
//      fillNullValue(row.getString(7))(_.toDouble)(_ => 0),
//      fillNullValue(row.getString(8))(_.toDouble)(_ => 0),
//      fillNullValue(row.getString(9))(_.toDouble)(_ => 0),
//      fillNullValue(row.getString(10))(_.toInt)(_ => 0),
//      fillNullValue(row.getString(11))(_.toInt)(_ => 0),
//      fillNullValue(row.getString(12))(_.toDouble)(_ => 0),
//      fillNullValue(row.getString(13))(_.toInt)(_ => 0),
//      row.getString(14),
//      row.getString(15)
//    )
//  }
//
//  private def rowsToSales(rows: Iterator[Row]): Iterator[Sales] = {
//    rows.map(this.rowToSales)
//  }
//
//  private def fillNullValue[A, B](value: A)(fn: A => B)(fillFn: A => B): B = {
//    value match {
//      case null => fillFn(value)
//      case s =>
//        try {
//          fn(s)
//        } catch {
//          case t: Throwable => fillFn(value)
//        }
//    }
//  }
}

case class Sales(name: String,
                 platform: String,
                 year_of_release: Int,
                 genre: String,
                 publisher: String,
                 na_sales: Double,
                 eu_sales: Double,
                 jp_Sales: Double,
                 other_sales: Double,
                 global_sales: Double,
                 critic_score: Int,
                 critic_count: Int,
                 user_score: Double,
                 user_count: Int,
                 developer: String,
                 rating: String)