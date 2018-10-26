//package com.example.log.source
//
//import com.example.utils.Spark
//import com.typesafe.scalalogging.LazyLogging
//import org.apache.spark.sql.functions.{from_unixtime, _}
//import org.apache.spark.sql.{DataFrame, Dataset, Row}
//import org.joda.time.DateTime
//
///**
//  * pb user list source DataFrame
//  *
//  * sub source list (app.conf)
//  * -userList,
//  * -nruList,
//  * -uvList,
//  * -clanUserList,
//  * -clanNRUList,
//  * -clanUVList,
//  * -nonClanUserList,
//  * -nonClanNRUList,
//  * -nonClanUVList
//  *
//  * create day, month, year source data
//  * read day, month, year source DataFrame
//  *
//  * Created by charles on 9/4/17.
//  */
//object UserList extends LazyLogging {
//  private val spark = Spark.getSparkSession()
//
//  private val projectName = "pb"
//  private val sourceName = "userList"
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
//    * @param date target month datetime
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
//    * @param date target year datetime
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
//  def writeDay(country:String, date:DateTime): DataFrame = {
//    logger.info(s"create ${projectName} ${sourceName} : ${date.toString("yyyyMMdd")}")
//
//    val userListPath: String = Util.getHdfsSourceDayPath(projectName, country, sourceName, "userList", date)
//    val nruListPath: String = Util.getHdfsSourceDayPath(projectName, country, sourceName, "nruList", date)
//    val uvListPath: String = Util.getHdfsSourceDayPath(projectName, country, sourceName, "uvList", date)
//    val clanUserListPath: String = Util.getHdfsSourceDayPath(projectName, country, sourceName, "clanUserList", date)
//    val clanNRUListPath: String = Util.getHdfsSourceDayPath(projectName, country, sourceName, "clanNRUList", date)
//    val clanUVListPath: String = Util.getHdfsSourceDayPath(projectName, country, sourceName, "clanUVList", date)
//    val nonClanUserListPath: String = Util.getHdfsSourceDayPath(projectName, country, sourceName, "nonClanUserList", date)
//    val nonClanNRUListPath: String = Util.getHdfsSourceDayPath(projectName, country, sourceName, "nonClanNRUList", date)
//    val nonClanUVListPath: String = Util.getHdfsSourceDayPath(projectName, country, sourceName, "nonClanUVList", date)
//
//    val pbGZLog1001DF: DataFrame = pbGZLog.getGZLogDayDF(country, "1001", date)
//    val pbGZLog1002DF: DataFrame = pbGZLog.getGZLogDayDF(country, "1002", date)
//    val pbGZLog1003DF: DataFrame = pbGZLog.getGZLogDayDF(country, "1003", date)
//
//    pbGZLog1001DF.cache()
//
//    val loginUserListDF: Dataset[Row] = pbGZLog1001DF
//      .filter(!$"sdata2".startsWith("gametest"))
//      .select(to_date($"logdate") as "date", $"ldata1" as "uid")
//      .distinct
//
//    val nruListDF: Dataset[Row] = pbGZLog1003DF
//      .filter($"idata1" === 1)
//      .select(to_date($"logdate") as "date", $"ldata1" as "uid")
//      .distinct
//
//    val uvListDF: Dataset[Row] = loginUserListDF.except(nruListDF)
//
//    val clanUserListDF: Dataset[Row] = pbGZLog1001DF
//      .filter($"idata6" =!= 0)
//      .select(to_date($"logdate") as "date", $"ldata1" as "uid")
//      .distinct
//      .union(
//        pbGZLog1002DF
//          .filter($"idata6" =!= 0)
//          .select(to_date($"logdate") as "date", $"ldata1" as "uid")
//          .distinct
//      ).distinct
//
//    val nonClanUserListDF: Dataset[Row] = loginUserListDF.except(clanUserListDF)
//
//    logger.info(s"save ${projectName} ${sourceName} userList : ${userListPath}")
//    Spark.writeToAvro(loginUserListDF, country, userListPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} nrucist : ${nruListPath}")
//    Spark.writeToAvro(nruListDF, country, nruListPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} uv${sourceName} ist : ${uvListPath}")
//    Spark.writeToAvro(uvListDF, country, uvListPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} clanUserList : ${clanUserListPath}")
//    Spark.writeToAvro(clanUserListDF, country, clanUserListPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} clanNRUList : ${clanNRUListPath}")
//    Spark.writeToAvro(
//      clanUserListDF.join(nruListDF, Seq("date", "uid")),
//      country,
//      clanNRUListPath,
//      "overwrite"
//    )
//
//    logger.info(s"save ${projectName} ${sourceName} clanUVList : ${clanUVListPath}")
//    Spark.writeToAvro(
//      clanUserListDF.join(uvListDF, Seq("date", "uid")),
//      country,
//      clanUVListPath,
//      "overwrite"
//    )
//
//    logger.info(s"save ${projectName} ${sourceName} nonClanUserList : ${nonClanUserListPath}")
//    Spark.writeToAvro(nonClanUserListDF, country, nonClanUserListPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} nonClanNRUList : ${nonClanNRUListPath}")
//    Spark.writeToAvro(
//      nonClanUserListDF.join(nruListDF, Seq("date", "uid")),
//      country,
//      nonClanNRUListPath, "overwrite"
//    )
//
//    logger.info(s"save ${projectName} ${sourceName} nonClanUVList : ${nonClanUVListPath}")
//    Spark.writeToAvro(
//      nonClanUserListDF.join(uvListDF,Seq("date", "uid")),
//      country,
//      nonClanUVListPath, "overwrite"
//    )
//
//    pbGZLog1001DF.unpersist()
//  }
//
//  /**
//    * write month source data
//    *
//    * @param country checked country name
//    * @param date target month datetime
//    * @return
//    */
//  def writeMonth(country:String, date:DateTime): DataFrame = {
//    logger.info(s"create ${projectName} ${sourceName} : ${date.toString("yyyyMM")}")
//
//    val userListPath = Util.getHdfsSourceMonthPath(projectName, country, sourceName, "userList", date)
//    val nruListPath = Util.getHdfsSourceMonthPath(projectName, country, sourceName, "nruList", date)
//    val uvListPath = Util.getHdfsSourceMonthPath(projectName, country, sourceName, "uvList", date)
//    val clanUserListPath = Util.getHdfsSourceMonthPath(projectName, country, sourceName, "clanUserList", date)
//    val clanNRUListPath = Util.getHdfsSourceMonthPath(projectName, country, sourceName, "clanNRUList", date)
//    val clanUVListPath = Util.getHdfsSourceMonthPath(projectName, country, sourceName, "clanUVList", date)
//    val nonClanUserListPath = Util.getHdfsSourceMonthPath(projectName, country, sourceName, "nonClanUserList", date)
//    val nonClanNRUListPath = Util.getHdfsSourceMonthPath(projectName, country, sourceName, "nonClanNRUList", date)
//    val nonClanUVListPath = Util.getHdfsSourceMonthPath(projectName, country, sourceName, "nonClanUVList", date)
//
//    val pbGZLog1001DF: DataFrame = pbGZLog.getGZLogMonthDF(country, "1001", date)
//    val pbGZLog1002DF: DataFrame = pbGZLog.getGZLogMonthDF(country, "1002", date)
//    val pbGZLog1003DF: DataFrame = pbGZLog.getGZLogMonthDF(country, "1003", date)
//
//    pbGZLog1001DF.cache()
//
//    val loginUserListDF: Dataset[Row] = pbGZLog1001DF
//      .filter(!$"sdata2".startsWith("gametest"))
//      .select(to_date($"logdate") as "date", $"ldata1" as "uid")
//      .distinct
//
//    val nruListDF: Dataset[Row] = pbGZLog1003DF
//      .filter($"idata1" === 1)
//      .select(to_date($"logdate") as "date", $"ldata1" as "uid")
//      .distinct
//
//    val uvListDF: Dataset[Row] = loginUserListDF.except(nruListDF)
//
//    val clanUserListDF: Dataset[Row] = pbGZLog1001DF
//      .filter($"idata6" =!= 0)
//      .select(to_date($"logdate") as "date", $"ldata1" as "uid")
//      .distinct
//      .union(
//        pbGZLog1002DF
//          .filter($"idata6" =!= 0)
//          .select(to_date($"logdate") as "date", $"ldata1" as "uid")
//          .distinct
//      ).distinct
//
//    val nonClanUserListDF: Dataset[Row] = loginUserListDF.except(clanUserListDF)
//
//    logger.info(s"save ${projectName} ${sourceName} userList : ${userListPath}")
//    Spark.writeToAvro(loginUserListDF, country, userListPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} nrucist : ${nruListPath}")
//    Spark.writeToAvro(nruListDF, country, nruListPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} uv${sourceName} ist : ${uvListPath}")
//    Spark.writeToAvro(uvListDF, country, uvListPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} clanUserList : ${clanUserListPath}")
//    Spark.writeToAvro(clanUserListDF, country, clanUserListPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} clanNRUList : ${clanNRUListPath}")
//    Spark.writeToAvro(
//      clanUserListDF.join(nruListDF, Seq("date", "uid")),
//      country,
//      clanNRUListPath,
//      "overwrite"
//    )
//
//    logger.info(s"save ${projectName} ${sourceName} clanUVList : ${clanUVListPath}")
//    Spark.writeToAvro(
//      clanUserListDF.join(uvListDF, Seq("date", "uid")),
//      country,
//      clanUVListPath,
//      "overwrite"
//    )
//
//    logger.info(s"save ${projectName} ${sourceName} nonClanUserList : ${nonClanUserListPath}")
//    Spark.writeToAvro(nonClanUserListDF, country, nonClanUserListPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} nonClanNRUList : ${nonClanNRUListPath}")
//    Spark.writeToAvro(
//      nonClanUserListDF.join(nruListDF, Seq("date", "uid")),
//      country,
//      nonClanNRUListPath, "overwrite"
//    )
//
//    logger.info(s"save ${projectName} ${sourceName} nonClanUVList : ${nonClanUVListPath}")
//    Spark.writeToAvro(
//      nonClanUserListDF.join(uvListDF,Seq("date", "uid")),
//      country,
//      nonClanUVListPath, "overwrite"
//    )
//
//    pbGZLog1001DF.unpersist()
//
//  }
//
//  /**
//    * write year source data
//    *
//    * @param country checked country name
//    * @param date target year datetime
//    * @return
//    */
//  def writeYear(country:String, date:DateTime): DataFrame = {
//    logger.info(s"create ${projectName} ${sourceName} : ${date.toString("yyyy")}")
//
//    val userListPath = Util.getHdfsSourceYearPath(projectName, country, sourceName, "userList", date)
//    val nruListPath = Util.getHdfsSourceYearPath(projectName, country, sourceName, "nruList", date)
//    val uvListPath = Util.getHdfsSourceYearPath(projectName, country, sourceName, "uvList", date)
//    val clanUserListPath = Util.getHdfsSourceYearPath(projectName, country, sourceName, "clanUserList", date)
//    val clanNRUListPath = Util.getHdfsSourceYearPath(projectName, country, sourceName, "clanNRUList", date)
//    val clanUVListPath = Util.getHdfsSourceYearPath(projectName, country, sourceName, "clanUVList", date)
//    val nonClanUserListPath = Util.getHdfsSourceYearPath(projectName, country, sourceName, "nonClanUserList", date)
//    val nonClanNRUListPath = Util.getHdfsSourceYearPath(projectName, country, sourceName, "nonClanNRUList", date)
//    val nonClanUVListPath = Util.getHdfsSourceYearPath(projectName, country, sourceName, "nonClanUVList", date)
//
//    val pbGZLog1001DF: DataFrame = pbGZLog.getGZLogYearDF(country, "1001", date)
//    val pbGZLog1002DF: DataFrame = pbGZLog.getGZLogYearDF(country, "1002", date)
//    val pbGZLog1003DF: DataFrame = pbGZLog.getGZLogYearDF(country, "1003", date)
//
//    pbGZLog1001DF.cache
//
//    val loginUserListDF: Dataset[Row] = pbGZLog1001DF
//      .filter(!$"sdata2".startsWith("gametest"))
//      .select(to_date($"logdate") as "date", $"ldata1" as "uid")
//      .distinct
//
//    val nruListDF: Dataset[Row] = pbGZLog1003DF
//      .filter($"idata1" === 1)
//      .select(to_date($"logdate") as "date", $"ldata1" as "uid")
//      .distinct
//
//    val uvListDF: Dataset[Row] = loginUserListDF.except(nruListDF)
//
//    val clanUserListDF: Dataset[Row] = pbGZLog1001DF
//      .filter($"idata6" =!= 0)
//      .select(to_date($"logdate") as "date", $"ldata1" as "uid")
//      .distinct
//      .union(
//        pbGZLog1002DF
//          .filter($"idata6" =!= 0)
//          .select(to_date($"logdate") as "date", $"ldata1" as "uid")
//          .distinct
//      ).distinct
//
//    val nonClanUserListDF: Dataset[Row] = loginUserListDF.except(clanUserListDF)
//
//    logger.info(s"save ${projectName} ${sourceName} userList : ${userListPath}")
//    Spark.writeToAvro(loginUserListDF, country, userListPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} nrucist : ${nruListPath}")
//    Spark.writeToAvro(nruListDF, country, nruListPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} uv${sourceName} ist : ${uvListPath}")
//    Spark.writeToAvro(uvListDF, country, uvListPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} clanUserList : ${clanUserListPath}")
//    Spark.writeToAvro(clanUserListDF, country, clanUserListPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} clanNRUList : ${clanNRUListPath}")
//    Spark.writeToAvro(
//      clanUserListDF.join(nruListDF, Seq("date", "uid")),
//      country,
//      clanNRUListPath,
//      "overwrite"
//    )
//
//    logger.info(s"save ${projectName} ${sourceName} clanUVList : ${clanUVListPath}")
//    Spark.writeToAvro(
//      clanUserListDF.join(uvListDF, Seq("date", "uid")),
//      country,
//      clanUVListPath,
//      "overwrite"
//    )
//
//    logger.info(s"save ${projectName} ${sourceName} nonClanUserList : ${nonClanUserListPath}")
//    Spark.writeToAvro(nonClanUserListDF, country, nonClanUserListPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} nonClanNRUList : ${nonClanNRUListPath}")
//    Spark.writeToAvro(
//      nonClanUserListDF.join(nruListDF, Seq("date", "uid")),
//      country,
//      nonClanNRUListPath, "overwrite"
//    )
//
//    logger.info(s"save ${projectName} ${sourceName} nonClanUVList : ${nonClanUVListPath}")
//    Spark.writeToAvro(
//      nonClanUserListDF.join(uvListDF,Seq("date", "uid")),
//      country,
//      nonClanUVListPath, "overwrite"
//    )
//
//    pbGZLog1001DF.unpersist()
//  }
//}
