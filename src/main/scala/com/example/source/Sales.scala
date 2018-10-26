package com.example.source

import com.example.config.HdfsConfig
import com.example.utils.Spark
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.B
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object Sales extends LazyLogging {
  private val exampleSourceFileName = "Video_Games_Sales_as_at_22_Dec_2016.csv"
  private val exampleSourceFilePath = s"${HdfsConfig.uri}/tmp/$exampleSourceFileName"

  private val spark = Spark.sparkSession
  import spark.implicits._

  def getDFFromCSVSource(csvSourcePath: String): DataFrame = {
    val df = spark.read.option("header", "true").csv(csvSourcePath)

    val nullFillMap: Map[String, Any] = Map(
      "Year_of_Release" -> -1,
      "NA_Sales" -> -1.0,
      "EU_Sales" -> -1.0,
      "JP_Sales" -> -1.0,
      "Other_Sales" -> -1.0,
      "Global_Sales" -> -1.0,
      "Critic_Score" -> -1,
      "Critic_Count" -> -1,
      "User_Score" -> -1.0,
      "User_Count" -> -1
    )

    df.na.fill(nullFillMap)

    df
  }

  def getDSFromCSVSource(csvSourcePath: String): Dataset[Sales] = {
    getDFFromCSVSource(csvSourcePath).mapPartitions(rowsToSales)
  }

  private def rowToSales(row: Row): Sales = {
    Sales(row.getString(0),
      row.getString(1),
      fillNullValue(row.getString(2))(_.toInt)(_ => 0),
      row.getString(3),
      row.getString(4),
      fillNullValue(row.getString(5))(_.toDouble)(_ => 0),
      fillNullValue(row.getString(6))(_.toDouble)(_ => 0),
      fillNullValue(row.getString(7))(_.toDouble)(_ => 0),
      fillNullValue(row.getString(8))(_.toDouble)(_ => 0),
      fillNullValue(row.getString(9))(_.toDouble)(_ => 0),
      fillNullValue(row.getString(10))(_.toInt)(_ => 0),
      fillNullValue(row.getString(11))(_.toInt)(_ => 0),
      fillNullValue(row.getString(12))(_.toDouble)(_ => 0),
      fillNullValue(row.getString(13))(_.toInt)(_ => 0),
      row.getString(14),
      row.getString(15)
    )
  }

  private def rowsToSales(rows: Iterator[Row]): Iterator[Sales] = {
    rows.map(this.rowToSales)
  }

  private def fillNullValue[B](value: String)(fn: String => B)(fillFn: String => B): B = {
    value match {
      case null => fillFn(value)
      case s => fn(s)
    }
  }
}

case class Sales(name: String,
                 flatForm: String,
                 yearOfRelease: Int,
                 genre: String,
                 publisher: String,
                 naSales: Double,
                 euSales: Double,
                 jpSales: Double,
                 otherSales: Double,
                 globalSales: Double,
                 criticScore: Int,
                 criticCount: Int,
                 userScore: Double,
                 userCount: Int,
                 developer: String,
                 rating: String)