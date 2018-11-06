package com.example.apps

import com.example.config.HdfsConfig
import com.example.source.{Sales, SalesSummary}
import com.example.utils.Spark
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.metrics.source
import org.apache.spark.sql.functions._
import vegas.DSL.Vegas

import scala.io.StdIn

import vegas._
import vegas.sparkExt._
import vegas.render.WindowRenderer._

/**
  * Name : Name of the game
  * Platform : Console on which the game is running
  * Year_of_Release : Year of the game released
  * Genre : Game's category
  * Publisher : Publisher
  * NA_Sales : Game sales in North America (in millions of units)
  * EU_Sales : Game sales in the European Union (in millions of units)
  * JP_Sales : Game sales in Japan (in millions of units)
  * Other_Sales : Game sales in the rest of the world, i.e. Africa, Asia excluding Japan, Australia, Europe excluding the E.U. and South America (in millions of units)
  * Global_Sales : Total sales in the world (in millions of units)
  * Critic_Score : Aggregate score compiled by Metacritic staff
  * Critic_Count : The number of critics used in coming up with the Critic_score
  * User_Score : Score by Metacritic's subscribers
  * User_Count : Number of users who gave the user_score
  * Developer : Party responsible for creating the game
  * Rating : The ESRB ratings (E.g. Everyone, Teen, Adults Only..etc)
  */

object AppMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val spark = Spark.sparkSession
    import spark.implicits._

    logger.info("app main")

    val exampleSourceFileName = "Video_Games_Sales_as_at_22_Dec_2016.csv"
    val exampleSourceFilePath = s"${HdfsConfig.uri}/tmp/$exampleSourceFileName"

    //val df = Sales.getRawDataSource
    val source = Sales.getInferSchemaDF

    SalesSummary.getYearOfReleaseStats(source).show
    SalesSummary.getYearOfReleaseStatsWithDeveloper(source).show
    SalesSummary.getYearOfReleaseStatsWithGenre(source).show
    SalesSummary.getYearOfReleaseStatsWithPlatForm(source).show
    SalesSummary.getYearOfReleaseStatsWithPublisher(source).show

    Vegas("genre year trend")
      .withDataFrame(SalesSummary.getYearOfReleaseStats(source))
      .encodeX("Year_of_Release", Nom)
      .encodeY("global_sales_sum", Quant)
      .mark(Bar)
      .show


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
