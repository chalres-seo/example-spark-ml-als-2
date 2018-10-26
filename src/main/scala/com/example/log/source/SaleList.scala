//package com.example.log.source
//
//import com.example.utils.Spark
//import com.typesafe.scalalogging.LazyLogging
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.functions.{from_unixtime, to_date, _}
//import org.joda.time.DateTime
//
///**
//  * pb sale list source DataFrame
//  *
//  * sub source list (app.conf)
//  * -userBuyItemList,
//  * -goodsSaleList
//  *
//  * create day, month, year source data
//  * read day, month, year source DataFrame
//  *
//  * Created by charles on 9/29/17.
//  */
//object SaleList extends LazyLogging {
//  private val spark = Spark.getSparkSession()
//
//  private val projectName = "pb"
//  private val sourceName = "saleList"
//  private val sourceList = Util.getSourceSubList(projectName, sourceName)
//  private val pbGZLog = new GZLog(projectName)
//
//  /**
//    * read day source data
//    *
//    * @param country checked country name
//    * @param sourceSubName read sub source name
//    * @param date target day datetime
//    * @return
//    */
//  def readDay(country:String, sourceSubName:String, date:DateTime): DataFrame = {
//    if(!sourceList.contains(sourceSubName)) {
//      logger.error(s"unknown source name ${sourceSubName}")
//      return null
//    }
//    val listPath: String = Util.getHdfsSourceDayPath(projectName, country, sourceName, sourceSubName, date)
//
//    logger.info(s"read ${projectName} ${sourceName} ${sourceSubName} : ${listPath}")
//
//    spark.read
//      .avro(listPath)
//      .withColumn("date", from_unixtime($"date" / 1000).cast("date"))
//  }
//
//  /**
//    * read month source data
//    *
//    * @param country checked country name
//    * @param sourceSubName read sub source name
//    * @param date target day datetime
//    * @return
//    */
//  def readMonth(country:String, sourceSubName:String, date:DateTime): DataFrame = {
//    if(!sourceList.contains(sourceSubName)) {
//      logger.error(s"unknown source name ${sourceSubName}")
//      return null
//    }
//    val listPath: String = Util.getHdfsSourceMonthPath(projectName, country, sourceName, sourceSubName, date)
//
//    logger.info(s"read ${projectName} ${sourceName} ${sourceSubName} : ${listPath}")
//
//    spark.read
//      .avro(listPath)
//      .withColumn("date", from_unixtime($"date" / 1000).cast("date"))
//  }
//
//  /**
//    * read year source data
//    *
//    * @param country checked country name
//    * @param sourceSubName read sub source name
//    * @param date target day datetime
//    * @return
//    */
//  def readYear(country:String, sourceSubName:String, date:DateTime): DataFrame = {
//    if(!sourceList.contains(sourceSubName)) {
//      logger.error(s"unknown source name ${sourceSubName}")
//      return null
//    }
//    val listPath: String = Util.getHdfsSourceYearPath(projectName, country, sourceName, sourceSubName, date)
//
//    logger.info(s"read ${projectName} ${sourceName} ${sourceSubName} : ${listPath}")
//
//    spark.read
//      .avro(listPath)
//      .withColumn("date", from_unixtime($"date" / 1000).cast("date"))
//  }
//
//  /**
//    * write day source data
//    *
//    * @param country checked country name
//    * @param date target day datetime
//    */
//  def writeDay(country:String, date:DateTime): Unit = {
//    logger.info(s"create ${projectName} ${sourceName} ${date.toString("yyyyMMdd")}")
//
//    val userBuyItemPath = Util.getHdfsSourceDayPath(projectName, country, sourceName, "userBuyItemList", date)
//    val salesItemPath = Util.getHdfsSourceDayPath(projectName, country, sourceName, "goodsSaleList", date)
//
//    val gzlog1103DF = pbGZLog.getGZLogDayDF(country, "1103", date)
//
//    val userBuyItemStatusDF = gzlog1103DF
//      .filter($"idata4" === 1)
//      .groupBy(to_date($"logdate") as "date", $"ldata1" as "uid", $"idata5" as "goodsId")
//      .agg(
//        count($"ldata1") as "cashSpendCount",
//        sum($"idata3") as "sumSpendCash",
//        min($"idata2") as "minTotalCash",
//        max($"idata2") as "maxTotalCash",
//        format_number(avg($"idata2"),2) as "avgTotalCash"
//      )
//
//    val goodsSaleStatusDF = userBuyItemStatusDF
//      .groupBy($"date", $"goodsId")
//      .agg(
//        countDistinct($"uid") as "buyUserCount",
//        min($"cashSpendCount") as "minBuyCountPerUser",
//        max($"cashSpendCount") as "maxBuyCountPerUser",
//        sum($"cashSpendCount") as "sumBuyCount",
//        //format_number(avg($"cashSpendCount"), 2) as "avgBuyCountPerUser",
//        sum($"sumSpendCash") as "sumSpendCash",
//        min($"minTotalCash") as "minTotalCash",
//        max($"maxTotalCash") as "maxTotalCash"
//      )
//
//    logger.info(s"save ${projectName} ${sourceName} userBuyItemList : ${userBuyItemPath}")
//    Spark.writeToAvro(userBuyItemStatusDF, country, userBuyItemPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} saleItemList : ${salesItemPath}")
//    Spark.writeToAvro(goodsSaleStatusDF, country, salesItemPath, "overwrite")
//  }
//
//  /**
//    * write month source data
//    *
//    * @param country checked country name
//    * @param date target day datetime
//    */
//  def writeMonth(country:String, date:DateTime): Unit = {
//    logger.info(s"create ${projectName} ${sourceName} ${date.toString("yyyyMM")}")
//
//    val userBuyItemPath = Util.getHdfsSourceMonthPath(projectName, country, sourceName, "userBuyItemList", date)
//    val salesItemPath = Util.getHdfsSourceMonthPath(projectName, country, sourceName, "goodsSaleList", date)
//
//    val gzlog1103DF = pbGZLog.getGZLogMonthDF(country, "1103", date)
//
//    val userBuyItemStatusDF = gzlog1103DF
//      .filter($"idata4" === 1)
//      .groupBy(to_date($"logdate") as "date", $"ldata1" as "uid", $"idata5" as "goodsId")
//      .agg(
//        count($"ldata1") as "cashSpendCount",
//        sum($"idata3") as "sumSpendCash",
//        min($"idata2") as "minTotalCash",
//        max($"idata2") as "maxTotalCash",
//        format_number(avg($"idata2"),2) as "avgTotalCash"
//      )
//
//    val goodsSaleStatusDF = userBuyItemStatusDF
//      .groupBy($"date", $"goodsId")
//      .agg(
//        countDistinct($"uid") as "buyUserCount",
//        min($"cashSpendCount") as "minBuyCountPerUser",
//        max($"cashSpendCount") as "maxBuyCountPerUser",
//        sum($"cashSpendCount") as "sumBuyCount",
//        //format_number(avg($"cashSpendCount"), 2) as "avgBuyCountPerUser",
//        sum($"sumSpendCash") as "sumSpendCash",
//        min($"minTotalCash") as "minTotalCash",
//        max($"maxTotalCash") as "maxTotalCash"
//      )
//
//    logger.info(s"save ${projectName} ${sourceName} userBuyItemList : ${userBuyItemPath}")
//    Spark.writeToAvro(userBuyItemStatusDF, country, userBuyItemPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} saleItemList : ${salesItemPath}")
//    Spark.writeToAvro(goodsSaleStatusDF, country, salesItemPath, "overwrite")
//  }
//
//  /**
//    * write month source data
//    *
//    * @param country checked country name
//    * @param date target day datetime
//    */
//  def writeYear(country:String, date:DateTime): Unit = {
//    logger.info(s"create ${projectName} ${sourceName} ${date.toString("yyyyMMdd")}")
//
//    val userBuyItemPath = Util.getHdfsSourceYearPath(projectName, country, sourceName, "userBuyItemList", date)
//    val salesItemPath = Util.getHdfsSourceYearPath(projectName, country, sourceName, "goodsSaleList", date)
//
//    val gzlog1103DF = pbGZLog.getGZLogYearDF(country, "1103", date)
//
//    val userBuyItemStatusDF = gzlog1103DF
//      .filter($"idata4" === 1)
//      .groupBy(to_date($"logdate") as "date", $"ldata1" as "uid", $"idata5" as "goodsId")
//      .agg(
//        count($"ldata1") as "cashSpendCount",
//        sum($"idata3") as "sumSpendCash",
//        min($"idata2") as "minTotalCash",
//        max($"idata2") as "maxTotalCash",
//        format_number(avg($"idata2"),2) as "avgTotalCash"
//      )
//
//    val goodsSaleStatusDF = userBuyItemStatusDF
//      .groupBy($"date", $"goodsId")
//      .agg(
//        countDistinct($"uid") as "buyUserCount",
//        min($"cashSpendCount") as "minBuyCountPerUser",
//        max($"cashSpendCount") as "maxBuyCountPerUser",
//        sum($"cashSpendCount") as "sumBuyCount",
//        //format_number(avg($"cashSpendCount"), 2) as "avgBuyCountPerUser",
//        sum($"sumSpendCash") as "sumSpendCash",
//        min($"minTotalCash") as "minTotalCash",
//        max($"maxTotalCash") as "maxTotalCash"
//      )
//
//    logger.info(s"save ${projectName} ${sourceName} userBuyItemList : ${userBuyItemPath}")
//    Spark.writeToAvro(userBuyItemStatusDF, country, userBuyItemPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} saleItemList : ${salesItemPath}")
//    Spark.writeToAvro(goodsSaleStatusDF, country, salesItemPath, "overwrite")
//  }
//
//}
