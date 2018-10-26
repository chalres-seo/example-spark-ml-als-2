package com.example.apps

import com.example.config.HdfsConfig
import com.example.source.Sales
import com.example.utils.Spark
import com.typesafe.scalalogging.LazyLogging

import scala.io.StdIn

object AppMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    import Spark.sparkSession.implicits._
    import org.apache.spark.sql.functions._

    logger.info("app main")

    val exampleSourceFileName = "Video_Games_Sales_as_at_22_Dec_2016.csv"
    val exampleSourceFilePath = s"${HdfsConfig.uri}/tmp/$exampleSourceFileName"

    val sourceDF = Sales.getDFFromCSVSource(exampleSourceFilePath)
    val sourceDS = Sales.getDSFromCSVSource(exampleSourceFilePath)

    sourceDF.printSchema()
    sourceDF.show(10)

    sourceDF.columns.foreach { col =>
      sourceDF.select(min(col), max(col)).show
    }

    //sourceDF.select(min($"Critic_Score"), max($"Critic_Score")).show

    sourceDS.printSchema()
    sourceDS.show(10)
    //this.waitConsoleForDebug
  }

  def waitConsoleForDebug: String = {
    println(">>> Press ENTER to exit <<<")
    try StdIn.readLine()
    finally {
      logger.debug("system exit.")
      System.exit(0)
    }
  }
}
