package com.example.source

import org.apache.spark.sql.{Column, DataFrame, Dataset, RelationalGroupedDataset}
import org.apache.spark.sql.functions._

object SalesSummary {
  import com.example.utils.Spark.sparkSession.implicits._

  def getYearOfReleaseStats(sourceDF: DataFrame): DataFrame = {
    this.aggSalesSum(sourceDF, $"Year_of_Release")
  }

  def getYearOfReleaseStatsWithPlatForm(sourceDF: DataFrame): DataFrame = {
    this.aggSalesSum(sourceDF, $"Year_of_Release", $"platform")
  }

  def getYearOfReleaseStatsWithGenre(sourceDF: DataFrame): DataFrame = {
    this.aggSalesSum(sourceDF, $"Year_of_Release", $"Genre")
  }

  def getYearOfReleaseStatsWithPublisher(sourceDF: DataFrame): DataFrame = {
    this.aggSalesSum(sourceDF, $"Year_of_Release", $"Publisher")
  }

  def getYearOfReleaseStatsWithDeveloper(sourceDF: DataFrame): DataFrame = {
    this.aggSalesSum(sourceDF, $"Year_of_Release", $"Developer")
  }



  private def aggSalesSum(df: DataFrame, groupColumns: Column*): DataFrame = {
    df.groupBy(groupColumns:_*)
      .agg(
//        sum(when($"Year_of_Release".isNotNull, 1).otherwise(0)).alias("year_of_release_count"),
//        sum(when($"Year_of_Release".isNull, 1).otherwise(0)).alias("year_of_release_null_count"),
        sum(when($"NA_Sales".isNotNull, 1).otherwise(0)).alias("na_sales_count"),
        format_number(sum("NA_Sales"), 2).alias("na_sales_sum"),
        sum(when($"EU_Sales".isNotNull, 1).otherwise(0)).alias("eu_sales_count"),
        format_number(sum("EU_Sales"), 2).alias("eu_sales_sum"),
        sum(when($"JP_Sales".isNotNull, 1).otherwise(0)).alias("jp_sales_count"),
        format_number(sum("JP_Sales"), 2).alias("jp_sales_sum"),
        sum(when($"Other_Sales".isNotNull, 1).otherwise(0)).alias("other_sales_count"),
        format_number(sum("Other_Sales"), 2).alias("other_sales_sum"),
        sum(when($"Global_Sales".isNotNull, 1).otherwise(0)).alias("global_sales_count"),
        format_number(sum("Global_Sales"), 2).alias("global_sales_sum")
      )
  }
}
