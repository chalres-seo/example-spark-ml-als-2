//package com.example.log.source
//
//import com.example.utils.Spark
//import com.typesafe.scalalogging.LazyLogging
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.functions._
//import org.joda.time.DateTime
//
//import scala.collection.mutable
//
///**
//  * pb user info source DataFrame
//  *
//  * sub source list (app.conf)
//  * -userStatus,
//  * -userInfo,
//  * -nruInfo,
//  * -uvInfo,
//  * -clanUserInfo,
//  * -clanNRUInfo,
//  * -clanUVInfo,
//  * -nonClanUserInfo,
//  * -nonClanNRUInfo,
//  * -nonClanUVInfo
//  *
//  * create day, month, year source data
//  * read day, month, year source DataFrame
//  *
//  * Created by charles on 9/29/17.
//  */
//object UserInfo extends LazyLogging {
//  private val spark = Spark.getSparkSession()
//
//  private val projectName = "pb"
//  private val sourceName = "userInfo"
//  private val sourceList: mutable.Buffer[String] = Util.getSourceSubList(projectName, sourceName)
//  private val pbGZLog: GZLog = new GZLog(projectName)
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
//    logger.info(s"create ${projectName} ${sourceName} : ${date.toString("yyyyMMdd")}")
//
//    val userStatusPath = Util.getHdfsSourceDayPath(projectName, country, sourceName, "userStatus", date)
//    val userInfoPath = Util.getHdfsSourceDayPath(projectName, country, sourceName, "userInfo", date)
//    val nruInfoPath = Util.getHdfsSourceDayPath(projectName, country, sourceName, "nruInfo", date)
//    val uvInfoPath = Util.getHdfsSourceDayPath(projectName, country, sourceName, "uvInfo", date)
//    val clanUserInfoPath = Util.getHdfsSourceDayPath(projectName, country, sourceName, "clanUserInfo", date)
//    val clanNRUInfoPath = Util.getHdfsSourceDayPath(projectName, country, sourceName, "clanNRUInfo", date)
//    val clanUVInfoPath = Util.getHdfsSourceDayPath(projectName, country, sourceName, "clanUVInfo", date)
//    val nonClanUserInfoPath = Util.getHdfsSourceDayPath(projectName, country, sourceName, "nonClanUserInfo", date)
//    val nonClanNRUInfoPath = Util.getHdfsSourceDayPath(projectName, country, sourceName, "nonClanNRUInfo", date)
//    val nonClanUVInfoPath = Util.getHdfsSourceDayPath(projectName, country, sourceName, "nonClanUVInfo", date)
//
//    val userListDF = UserList.readDay(country, "userList", date)
//
//    val gzlog1001DF = pbGZLog.getGZLogDayDF(country, "1001", date)
//    val gzlog1002DF = pbGZLog.getGZLogDayDF(country, "1002", date)
//    val gzlog1101DF = pbGZLog.getGZLogDayDF(country, "1101", date)
//    val gzlog1102DF = pbGZLog.getGZLogDayDF(country, "1102", date)
//    val gzlog1103DF = pbGZLog.getGZLogDayDF(country, "1103", date)
//
//    val nruListDF = UserList.readDay(country, "nruList", date)
//    val uvListDF = UserList.readDay(country, "uvList", date)
//    val clanUserListDF = UserList.readDay(country, "clanUserList", date)
//    val clanNRUListDF = UserList.readDay(country, "clanNRUList", date)
//    val clanUVListDF = UserList.readDay(country, "clanUVList", date)
//    val nonClanUserListDF = UserList.readDay(country, "nonClanUserList", date)
//    val nonClanNRUListDF = UserList.readDay(country, "nonClanNRUList", date)
//    val nonClanUVListDF = UserList.readDay(country, "nonClanUVList", date)
//
//    val userGetPointInfoDF = gzlog1101DF
//      .withColumn("date", to_date($"logdate"))
//      .groupBy($"date", $"ldata1" as "uid")
//      .agg(
//        count($"ldata1") as "getPointCount",
//        sum($"idata3") as "totalGetPoint",
//        min($"idata3") as "minGetPoint",
//        max($"idata3") as "maxGetPoint",
//        format_number(avg($"idata3"), 2) as "avgGetPoint"
//      )
//
//    val userSpendPointInfoDF = gzlog1102DF
//      .withColumn("date", to_date($"logdate"))
//      .groupBy($"date", $"ldata1" as "uid")
//      .agg(
//        count($"ldata1") as "spendPointCount",
//        sum($"idata3") as "totalSpendPoint",
//        min($"idata3") as "minSpendPoint",
//        max($"idata3") as "maxSpendPoint",
//        format_number(avg($"idata3"), 2) as "avgSpendPoint"
//      )
//
//    val userSpendCashInfoDF = gzlog1103DF
//      .withColumn("date", to_date($"logdate"))
//      .groupBy($"date", $"ldata1" as "uid")
//      .agg(
//        count($"ldata1") as "SpendCashCount",
//        count(when($"idata8" =!= 0, true)) as "clanSpendCashCount",
//        count(when($"idata8" === 0, true)) as "nonClanSpendCashCount",
//        sum($"idata3") as "totalSpendCash",
//        min($"idata3") as "minSpendCash",
//        max($"idata3") as "maxSpendCash",
//        format_number(avg($"idata3"), 2) as "avgSpendCash",
//        sum(when($"idata8" =!= 0, $"idata3")) as "clanTotalSpendCash",
//        min(when($"idata8" =!= 0, $"idata3")) as "clanMinSpendCash",
//        max(when($"idata8" =!= 0, $"idata3")) as "clanMaxSpendCash",
//        format_number(avg(when($"idata8" =!= 0, $"idata3")), 2) as "clanAvgSpendCash",
//        sum(when($"idata8" === 0, $"idata3")) as "nonClanTotalSpendCash",
//        min(when($"idata8" === 0, $"idata3")) as "nonClanMinSpendCash",
//        max(when($"idata8" === 0, $"idata3")) as "nonclanMaxSpendCash",
//        format_number(avg(when($"idata8" === 0, $"idata3")), 2) as "nonClanAvgSpendCash"
//      )
//
//    val userLoginInfoDF = gzlog1001DF
//      .filter(!$"sdata2".startsWith("gametest"))
//      .withColumn("date", to_date($"logdate"))
//      .groupBy($"date", $"ldata1" as "uid")
//      .agg(
//        count($"ldata1") as "loginCount",
//        count(when($"idata6" =!= 0, true)) as "clanLoginCount",
//        count(when($"idata6" === 0, true)) as "nonClanLoginCount",
//        count(when($"idata5" === 0, true)) as "nonCafeLoginCount",
//        count(when($"idata5" === 1, true)) as "cafeLoginCount",
//        count(when($"idata5" === 2, true)) as "premiumCafeLoginCount",
//        count(when($"idata5" === 3, true)) as "vacationEventCafeLoginCount",
//        count(when($"idata5" === 7, true)) as "garenCafeLoginCount",
//        count(when($"idata5" === 8, true)) as "garenaPlusCafeLoginCount",
//        min($"idata7") as "minTotalConnectTime",
//        max($"idata7") as "maxTotalConnectTime",
//        min($"idata1") as "minLoginRank",
//        max($"idata1") as "maxLoginRank",
//        min($"idata2") as "minLoginPoint",
//        max($"idata2") as "maxLoginPoint",
//        min($"idata3") as "minLoginCash",
//        max($"idata3") as "maxLoginCash",
//        min($"idata4") as "minLoginExp",
//        max($"idata4") as "maxLoginExp"
//      )
//
//    val userLogoutInfoDF = gzlog1002DF
//      .filter(!$"sdata2".startsWith("gametest"))
//      .withColumn("date", to_date($"logdate"))
//      .groupBy($"date", $"ldata1" as "uid")
//      .agg(
//        count($"ldata1") as "logoutCount",
//        count(when($"idata6" === 0, true)) as "nonClanLogoutCount",
//        count(when($"idata6" =!= 0, true)) as "clanLogoutCount",
//        count(when($"idata5" === 0, true)) as "nonCafeLogoutCount",
//        count(when($"idata5" === 1, true)) as "cafeLogoutCount",
//        count(when($"idata5" === 2, true)) as "premiumCafeLogoutCount",
//        count(when($"idata5" === 3, true)) as "vacationEventCafeLogoutCount",
//        count(when($"idata5" === 7, true)) as "garenaCafeLogoutCount",
//        count(when($"idata5" === 8, true)) as "garenaPlusCafeLogoutCount",
//        min($"idata7") as "minConnectTime",
//        max($"idata7") as "maxConnectTime",
//        sum($"idata7") as "sumConnectTime",
//        format_number(avg($"idata7"), 2) as "avgConnectTime",
//        min($"idata8") as "minBattleTime",
//        max($"idata8") as "maxBattleTime",
//        sum($"idata8") as "sumBattleTime",
//        format_number(avg($"idata8"), 2) as "avgBattleTime",
//        min($"idata1") as "minLogoutRank",
//        max($"idata1") as "maxLogoutRank",
//        min($"idata2") as "minLogoutPoint",
//        max($"idata2") as "maxLogoutPoint",
//        min($"idata3") as "minLogoutCash",
//        max($"idata3") as "maxLogoutCash",
//        min($"idata4") as "minLogoutExp",
//        max($"idata4") as "maxLogoutExp"
//      )
//
//    val userStatusDF = userListDF
//      .join(userGetPointInfoDF, Seq("date" , "uid"), "left_outer")
//      .join(userSpendPointInfoDF, Seq("date" , "uid"), "left_outer")
//      .join(userSpendCashInfoDF, Seq("date" , "uid"), "left_outer")
//      .join(userLoginInfoDF, Seq("date" , "uid"), "left_outer")
//      .join(userLogoutInfoDF, Seq("date" , "uid"), "left_outer")
//
//    userStatusDF.cache
//
//    val userInfoDF = userStatusDF
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val nruInfoDF = userStatusDF
//      .join(nruListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val uvInfoDF = userStatusDF
//      .join(uvListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val clanUserInfoDF = userStatusDF
//      .join(clanUserListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val clanNRUInfoDF = userStatusDF
//      .join(clanNRUListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val clanUVInfoDF = userStatusDF
//      .join(clanUVListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val nonClanUserInfoDF = userStatusDF
//      .join(nonClanUserListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val nonClanNRUInfoDF = userStatusDF
//      .join(nonClanNRUListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val nonClanUVInfoDF = userStatusDF
//      .join(nonClanUVListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    logger.info(s"save ${projectName} ${sourceName} userStatus : ${userStatusPath}")
//    Spark.writeToAvro(userStatusDF, country, userStatusPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} userInfo : ${userInfoPath}")
//    Spark.writeToAvro(userInfoDF, country, userInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} nruInfo : ${nruInfoPath}")
//    Spark.writeToAvro(nruInfoDF, country, nruInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} uvInfo : ${uvInfoPath}")
//    Spark.writeToAvro(uvInfoDF, country, uvInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} clanUserInfo : ${clanUserInfoPath}")
//    Spark.writeToAvro(clanUserInfoDF, country, clanUserInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} clanNRUInfo : ${clanNRUInfoPath}")
//    Spark.writeToAvro(clanNRUInfoDF, country, clanNRUInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} clanUVInfo : ${clanUVInfoPath}")
//    Spark.writeToAvro(clanUVInfoDF, country, clanUVInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} nonClanUserInfo : ${nonClanUserInfoPath}")
//    Spark.writeToAvro(nonClanUserInfoDF, country, nonClanUserInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} nonClanNRUInfo : ${nonClanNRUInfoPath}")
//    Spark.writeToAvro(nonClanNRUInfoDF, country, nonClanNRUInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} nonClanUVInfo : ${nonClanUVInfoPath}")
//    Spark.writeToAvro(nonClanUVInfoDF, country, nonClanUVInfoPath, "overwrite")
//
//  }
//
//  /**
//    * write month source data
//    *
//    * @param country checked country name
//    * @param date target day datetime
//    */
//  def writeMonth(country:String, date:DateTime): Unit = {
//    logger.info(s"create ${projectName} ${sourceName} : ${date.toString("yyyyMM")}")
//
//    val userStatusPath = Util.getHdfsSourceMonthPath(projectName, country, sourceName, "userStatus", date)
//    val userInfoPath = Util.getHdfsSourceMonthPath(projectName, country, sourceName, "userInfo", date)
//    val nruInfoPath = Util.getHdfsSourceMonthPath(projectName, country, sourceName, "nruInfo", date)
//    val uvInfoPath = Util.getHdfsSourceMonthPath(projectName, country, sourceName, "uvInfo", date)
//    val clanUserInfoPath = Util.getHdfsSourceMonthPath(projectName, country, sourceName, "clanUserInfo", date)
//    val clanNRUInfoPath = Util.getHdfsSourceMonthPath(projectName, country, sourceName, "clanNRUInfo", date)
//    val clanUVInfoPath = Util.getHdfsSourceMonthPath(projectName, country, sourceName, "clanUVInfo", date)
//    val nonClanUserInfoPath = Util.getHdfsSourceMonthPath(projectName, country, sourceName, "nonClanUserInfo", date)
//    val nonClanNRUInfoPath = Util.getHdfsSourceMonthPath(projectName, country, sourceName, "nonClanNRUInfo", date)
//    val nonClanUVInfoPath = Util.getHdfsSourceMonthPath(projectName, country, sourceName, "nonClanUVInfo", date)
//
//    val userListDF = UserList.readMonth(country, "userList", date)
//
//    val gzlog1001DF = pbGZLog.getGZLogMonthDF(country, "1001", date)
//    val gzlog1002DF = pbGZLog.getGZLogMonthDF(country, "1002", date)
//    val gzlog1101DF = pbGZLog.getGZLogMonthDF(country, "1101", date)
//    val gzlog1102DF = pbGZLog.getGZLogMonthDF(country, "1102", date)
//    val gzlog1103DF = pbGZLog.getGZLogMonthDF(country, "1103", date)
//
//    val nruListDF = UserList.readMonth(country, "nruList", date)
//    val uvListDF = UserList.readMonth(country, "uvList", date)
//    val clanUserListDF = UserList.readMonth(country, "clanUserList", date)
//    val clanNRUListDF = UserList.readMonth(country, "clanNRUList", date)
//    val clanUVListDF = UserList.readMonth(country, "clanUVList", date)
//    val nonClanUserListDF = UserList.readMonth(country, "nonClanUserList", date)
//    val nonClanNRUListDF = UserList.readMonth(country, "nonClanNRUList", date)
//    val nonClanUVListDF = UserList.readMonth(country, "nonClanUVList", date)
//
//    val userGetPointInfoDF = gzlog1101DF
//      .withColumn("date", to_date($"logdate"))
//      .groupBy($"date", $"ldata1" as "uid")
//      .agg(
//        count($"ldata1") as "getPointCount",
//        sum($"idata3") as "totalGetPoint",
//        min($"idata3") as "minGetPoint",
//        max($"idata3") as "maxGetPoint",
//        format_number(avg($"idata3"), 2) as "avgGetPoint"
//      )
//
//    val userSpendPointInfoDF = gzlog1102DF
//      .withColumn("date", to_date($"logdate"))
//      .groupBy($"date", $"ldata1" as "uid")
//      .agg(
//        count($"ldata1") as "spendPointCount",
//        sum($"idata3") as "totalSpendPoint",
//        min($"idata3") as "minSpendPoint",
//        max($"idata3") as "maxSpendPoint",
//        format_number(avg($"idata3"), 2) as "avgSpendPoint"
//      )
//
//    val userSpendCashInfoDF = gzlog1103DF
//      .withColumn("date", to_date($"logdate"))
//      .groupBy($"date", $"ldata1" as "uid")
//      .agg(
//        count($"ldata1") as "SpendCashCount",
//        count(when($"idata8" =!= 0, true)) as "clanSpendCashCount",
//        count(when($"idata8" === 0, true)) as "nonClanSpendCashCount",
//        sum($"idata3") as "totalSpendCash",
//        min($"idata3") as "minSpendCash",
//        max($"idata3") as "maxSpendCash",
//        format_number(avg($"idata3"), 2) as "avgSpendCash",
//        sum(when($"idata8" =!= 0, $"idata3")) as "clanTotalSpendCash",
//        min(when($"idata8" =!= 0, $"idata3")) as "clanMinSpendCash",
//        max(when($"idata8" =!= 0, $"idata3")) as "clanMaxSpendCash",
//        format_number(avg(when($"idata8" =!= 0, $"idata3")), 2) as "clanAvgSpendCash",
//        sum(when($"idata8" === 0, $"idata3")) as "nonClanTotalSpendCash",
//        min(when($"idata8" === 0, $"idata3")) as "nonClanMinSpendCash",
//        max(when($"idata8" === 0, $"idata3")) as "nonclanMaxSpendCash",
//        format_number(avg(when($"idata8" === 0, $"idata3")), 2) as "nonClanAvgSpendCash"
//      )
//
//    val userLoginInfoDF = gzlog1001DF
//      .filter(!$"sdata2".startsWith("gametest"))
//      .withColumn("date", to_date($"logdate"))
//      .groupBy($"date", $"ldata1" as "uid")
//      .agg(
//        count($"ldata1") as "loginCount",
//        count(when($"idata6" =!= 0, true)) as "clanLoginCount",
//        count(when($"idata6" === 0, true)) as "nonClanLoginCount",
//        count(when($"idata5" === 0, true)) as "nonCafeLoginCount",
//        count(when($"idata5" === 1, true)) as "cafeLoginCount",
//        count(when($"idata5" === 2, true)) as "premiumCafeLoginCount",
//        count(when($"idata5" === 3, true)) as "vacationEventCafeLoginCount",
//        count(when($"idata5" === 7, true)) as "garenCafeLoginCount",
//        count(when($"idata5" === 8, true)) as "garenaPlusCafeLoginCount",
//        min($"idata7") as "minTotalConnectTime",
//        max($"idata7") as "maxTotalConnectTime",
//        min($"idata1") as "minLoginRank",
//        max($"idata1") as "maxLoginRank",
//        min($"idata2") as "minLoginPoint",
//        max($"idata2") as "maxLoginPoint",
//        min($"idata3") as "minLoginCash",
//        max($"idata3") as "maxLoginCash",
//        min($"idata4") as "minLoginExp",
//        max($"idata4") as "maxLoginExp"
//      )
//
//    val userLogoutInfoDF = gzlog1002DF
//      .filter(!$"sdata2".startsWith("gametest"))
//      .withColumn("date", to_date($"logdate"))
//      .groupBy($"date", $"ldata1" as "uid")
//      .agg(
//        count($"ldata1") as "logoutCount",
//        count(when($"idata6" === 0, true)) as "nonClanLogoutCount",
//        count(when($"idata6" =!= 0, true)) as "clanLogoutCount",
//        count(when($"idata5" === 0, true)) as "nonCafeLogoutCount",
//        count(when($"idata5" === 1, true)) as "cafeLogoutCount",
//        count(when($"idata5" === 2, true)) as "premiumCafeLogoutCount",
//        count(when($"idata5" === 3, true)) as "vacationEventCafeLogoutCount",
//        count(when($"idata5" === 7, true)) as "garenaCafeLogoutCount",
//        count(when($"idata5" === 8, true)) as "garenaPlusCafeLogoutCount",
//        min($"idata7") as "minConnectTime",
//        max($"idata7") as "maxConnectTime",
//        sum($"idata7") as "sumConnectTime",
//        format_number(avg($"idata7"), 2) as "avgConnectTime",
//        min($"idata8") as "minBattleTime",
//        max($"idata8") as "maxBattleTime",
//        sum($"idata8") as "sumBattleTime",
//        format_number(avg($"idata8"), 2) as "avgBattleTime",
//        min($"idata1") as "minLogoutRank",
//        max($"idata1") as "maxLogoutRank",
//        min($"idata2") as "minLogoutPoint",
//        max($"idata2") as "maxLogoutPoint",
//        min($"idata3") as "minLogoutCash",
//        max($"idata3") as "maxLogoutCash",
//        min($"idata4") as "minLogoutExp",
//        max($"idata4") as "maxLogoutExp"
//      )
//
//    val userStatusDF = userListDF
//      .join(userGetPointInfoDF, Seq("date" , "uid"), "left_outer")
//      .join(userSpendPointInfoDF, Seq("date" , "uid"), "left_outer")
//      .join(userSpendCashInfoDF, Seq("date" , "uid"), "left_outer")
//      .join(userLoginInfoDF, Seq("date" , "uid"), "left_outer")
//      .join(userLogoutInfoDF, Seq("date" , "uid"), "left_outer")
//
//    userStatusDF.cache
//
//    val userInfoDF = userStatusDF
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val nruInfoDF = userStatusDF
//      .join(nruListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val uvInfoDF = userStatusDF
//      .join(uvListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val clanUserInfoDF = userStatusDF
//      .join(clanUserListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val clanNRUInfoDF = userStatusDF
//      .join(clanNRUListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val clanUVInfoDF = userStatusDF
//      .join(clanUVListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val nonClanUserInfoDF = userStatusDF
//      .join(nonClanUserListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val nonClanNRUInfoDF = userStatusDF
//      .join(nonClanNRUListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val nonClanUVInfoDF = userStatusDF
//      .join(nonClanUVListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    logger.info(s"save ${projectName} ${sourceName} userStatus : ${userStatusPath}")
//    Spark.writeToAvro(userStatusDF, country, userStatusPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} userInfo : ${userInfoPath}")
//    Spark.writeToAvro(userInfoDF, country, userInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} nruInfo : ${nruInfoPath}")
//    Spark.writeToAvro(nruInfoDF, country, nruInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} uvInfo : ${uvInfoPath}")
//    Spark.writeToAvro(uvInfoDF, country, uvInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} clanUserInfo : ${clanUserInfoPath}")
//    Spark.writeToAvro(clanUserInfoDF, country, clanUserInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} clanNRUInfo : ${clanNRUInfoPath}")
//    Spark.writeToAvro(clanNRUInfoDF, country, clanNRUInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} clanUVInfo : ${clanUVInfoPath}")
//    Spark.writeToAvro(clanUVInfoDF, country, clanUVInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} nonClanUserInfo : ${nonClanUserInfoPath}")
//    Spark.writeToAvro(nonClanUserInfoDF, country, nonClanUserInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} nonClanNRUInfo : ${nonClanNRUInfoPath}")
//    Spark.writeToAvro(nonClanNRUInfoDF, country, nonClanNRUInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} nonClanUVInfo : ${nonClanUVInfoPath}")
//    Spark.writeToAvro(nonClanUVInfoDF, country, nonClanUVInfoPath, "overwrite")
//
//  }
//
//  /**
//    * write year source data
//    *
//    * @param country checked country name
//    * @param date target day datetime
//    */
//  def writeYear(country:String, date:DateTime): Unit = {
//    logger.info(s"create ${projectName} ${sourceName} : ${date.toString("yyyy")}")
//
//    val userStatusPath = Util.getHdfsSourceYearPath(projectName, country, sourceName, "userStatus", date)
//    val userInfoPath = Util.getHdfsSourceYearPath(projectName, country, sourceName, "userInfo", date)
//    val nruInfoPath = Util.getHdfsSourceYearPath(projectName, country, sourceName, "nruInfo", date)
//    val uvInfoPath = Util.getHdfsSourceYearPath(projectName, country, sourceName, "uvInfo", date)
//    val clanUserInfoPath = Util.getHdfsSourceYearPath(projectName, country, sourceName, "clanUserInfo", date)
//    val clanNRUInfoPath = Util.getHdfsSourceYearPath(projectName, country, sourceName, "clanNRUInfo", date)
//    val clanUVInfoPath = Util.getHdfsSourceYearPath(projectName, country, sourceName, "clanUVInfo", date)
//    val nonClanUserInfoPath = Util.getHdfsSourceYearPath(projectName, country, sourceName, "nonClanUserInfo", date)
//    val nonClanNRUInfoPath = Util.getHdfsSourceYearPath(projectName, country, sourceName, "nonClanNRUInfo", date)
//    val nonClanUVInfoPath = Util.getHdfsSourceYearPath(projectName, country, sourceName, "nonClanUVInfo", date)
//
//    val userListDF = UserList.readYear(country, "userList", date)
//
//    val gzlog1001DF = pbGZLog.getGZLogYearDF(country, "1001", date)
//    val gzlog1002DF = pbGZLog.getGZLogYearDF(country, "1002", date)
//    val gzlog1101DF = pbGZLog.getGZLogYearDF(country, "1101", date)
//    val gzlog1102DF = pbGZLog.getGZLogYearDF(country, "1102", date)
//    val gzlog1103DF = pbGZLog.getGZLogYearDF(country, "1103", date)
//
//    val nruListDF = UserList.readYear(country, "nruList", date)
//    val uvListDF = UserList.readYear(country, "uvList", date)
//    val clanUserListDF = UserList.readYear(country, "clanUserList", date)
//    val clanNRUListDF = UserList.readYear(country, "clanNRUList", date)
//    val clanUVListDF = UserList.readYear(country, "clanUVList", date)
//    val nonClanUserListDF = UserList.readYear(country, "nonClanUserList", date)
//    val nonClanNRUListDF = UserList.readYear(country, "nonClanNRUList", date)
//    val nonClanUVListDF = UserList.readYear(country, "nonClanUVList", date)
//
//    val userGetPointInfoDF = gzlog1101DF
//      .withColumn("date", to_date($"logdate"))
//      .groupBy($"date", $"ldata1" as "uid")
//      .agg(
//        count($"ldata1") as "getPointCount",
//        sum($"idata3") as "totalGetPoint",
//        min($"idata3") as "minGetPoint",
//        max($"idata3") as "maxGetPoint",
//        format_number(avg($"idata3"), 2) as "avgGetPoint"
//      )
//
//    val userSpendPointInfoDF = gzlog1102DF
//      .withColumn("date", to_date($"logdate"))
//      .groupBy($"date", $"ldata1" as "uid")
//      .agg(
//        count($"ldata1") as "spendPointCount",
//        sum($"idata3") as "totalSpendPoint",
//        min($"idata3") as "minSpendPoint",
//        max($"idata3") as "maxSpendPoint",
//        format_number(avg($"idata3"), 2) as "avgSpendPoint"
//      )
//
//    val userSpendCashInfoDF = gzlog1103DF
//      .withColumn("date", to_date($"logdate"))
//      .groupBy($"date", $"ldata1" as "uid")
//      .agg(
//        count($"ldata1") as "SpendCashCount",
//        count(when($"idata8" =!= 0, true)) as "clanSpendCashCount",
//        count(when($"idata8" === 0, true)) as "nonClanSpendCashCount",
//        sum($"idata3") as "totalSpendCash",
//        min($"idata3") as "minSpendCash",
//        max($"idata3") as "maxSpendCash",
//        format_number(avg($"idata3"), 2) as "avgSpendCash",
//        sum(when($"idata8" =!= 0, $"idata3")) as "clanTotalSpendCash",
//        min(when($"idata8" =!= 0, $"idata3")) as "clanMinSpendCash",
//        max(when($"idata8" =!= 0, $"idata3")) as "clanMaxSpendCash",
//        format_number(avg(when($"idata8" =!= 0, $"idata3")), 2) as "clanAvgSpendCash",
//        sum(when($"idata8" === 0, $"idata3")) as "nonClanTotalSpendCash",
//        min(when($"idata8" === 0, $"idata3")) as "nonClanMinSpendCash",
//        max(when($"idata8" === 0, $"idata3")) as "nonclanMaxSpendCash",
//        format_number(avg(when($"idata8" === 0, $"idata3")), 2) as "nonClanAvgSpendCash"
//      )
//
//    val userLoginInfoDF = gzlog1001DF
//      .filter(!$"sdata2".startsWith("gametest"))
//      .withColumn("date", to_date($"logdate"))
//      .groupBy($"date", $"ldata1" as "uid")
//      .agg(
//        count($"ldata1") as "loginCount",
//        count(when($"idata6" =!= 0, true)) as "clanLoginCount",
//        count(when($"idata6" === 0, true)) as "nonClanLoginCount",
//        count(when($"idata5" === 0, true)) as "nonCafeLoginCount",
//        count(when($"idata5" === 1, true)) as "cafeLoginCount",
//        count(when($"idata5" === 2, true)) as "premiumCafeLoginCount",
//        count(when($"idata5" === 3, true)) as "vacationEventCafeLoginCount",
//        count(when($"idata5" === 7, true)) as "garenCafeLoginCount",
//        count(when($"idata5" === 8, true)) as "garenaPlusCafeLoginCount",
//        min($"idata7") as "minTotalConnectTime",
//        max($"idata7") as "maxTotalConnectTime",
//        min($"idata1") as "minLoginRank",
//        max($"idata1") as "maxLoginRank",
//        min($"idata2") as "minLoginPoint",
//        max($"idata2") as "maxLoginPoint",
//        min($"idata3") as "minLoginCash",
//        max($"idata3") as "maxLoginCash",
//        min($"idata4") as "minLoginExp",
//        max($"idata4") as "maxLoginExp"
//      )
//
//    val userLogoutInfoDF = gzlog1002DF
//      .filter(!$"sdata2".startsWith("gametest"))
//      .withColumn("date", to_date($"logdate"))
//      .groupBy($"date", $"ldata1" as "uid")
//      .agg(
//        count($"ldata1") as "logoutCount",
//        count(when($"idata6" === 0, true)) as "nonClanLogoutCount",
//        count(when($"idata6" =!= 0, true)) as "clanLogoutCount",
//        count(when($"idata5" === 0, true)) as "nonCafeLogoutCount",
//        count(when($"idata5" === 1, true)) as "cafeLogoutCount",
//        count(when($"idata5" === 2, true)) as "premiumCafeLogoutCount",
//        count(when($"idata5" === 3, true)) as "vacationEventCafeLogoutCount",
//        count(when($"idata5" === 7, true)) as "garenaCafeLogoutCount",
//        count(when($"idata5" === 8, true)) as "garenaPlusCafeLogoutCount",
//        min($"idata7") as "minConnectTime",
//        max($"idata7") as "maxConnectTime",
//        sum($"idata7") as "sumConnectTime",
//        format_number(avg($"idata7"), 2) as "avgConnectTime",
//        min($"idata8") as "minBattleTime",
//        max($"idata8") as "maxBattleTime",
//        sum($"idata8") as "sumBattleTime",
//        format_number(avg($"idata8"), 2) as "avgBattleTime",
//        min($"idata1") as "minLogoutRank",
//        max($"idata1") as "maxLogoutRank",
//        min($"idata2") as "minLogoutPoint",
//        max($"idata2") as "maxLogoutPoint",
//        min($"idata3") as "minLogoutCash",
//        max($"idata3") as "maxLogoutCash",
//        min($"idata4") as "minLogoutExp",
//        max($"idata4") as "maxLogoutExp"
//      )
//
//    val userStatusDF = userListDF
//      .join(userGetPointInfoDF, Seq("date" , "uid"), "left_outer")
//      .join(userSpendPointInfoDF, Seq("date" , "uid"), "left_outer")
//      .join(userSpendCashInfoDF, Seq("date" , "uid"), "left_outer")
//      .join(userLoginInfoDF, Seq("date" , "uid"), "left_outer")
//      .join(userLogoutInfoDF, Seq("date" , "uid"), "left_outer")
//
//    userStatusDF.cache
//
//    val userInfoDF = userStatusDF
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val nruInfoDF = userStatusDF
//      .join(nruListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val uvInfoDF = userStatusDF
//      .join(uvListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val clanUserInfoDF = userStatusDF
//      .join(clanUserListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val clanNRUInfoDF = userStatusDF
//      .join(clanNRUListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val clanUVInfoDF = userStatusDF
//      .join(clanUVListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val nonClanUserInfoDF = userStatusDF
//      .join(nonClanUserListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val nonClanNRUInfoDF = userStatusDF
//      .join(nonClanNRUListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    val nonClanUVInfoDF = userStatusDF
//      .join(nonClanUVListDF, Seq("date", "uid"))
//      .select(
//        $"date",
//        $"uid",
//        $"getPointCount",
//        $"totalGetPoint",
//        $"spendPointCount",
//        $"totalSpendPoint",
//        $"SpendCashCount",
//        $"clanSpendCashCount",
//        $"nonClanSpendCashCount",
//        $"totalSpendCash",
//        $"clanTotalSpendCash",
//        $"nonClanTotalSpendCash"
//      )
//
//    logger.info(s"save ${projectName} ${sourceName} userStatus : ${userStatusPath}")
//    Spark.writeToAvro(userStatusDF, country, userStatusPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} userInfo : ${userInfoPath}")
//    Spark.writeToAvro(userInfoDF, country, userInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} nruInfo : ${nruInfoPath}")
//    Spark.writeToAvro(nruInfoDF, country, nruInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} uvInfo : ${uvInfoPath}")
//    Spark.writeToAvro(uvInfoDF, country, uvInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} clanUserInfo : ${clanUserInfoPath}")
//    Spark.writeToAvro(clanUserInfoDF, country, clanUserInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} clanNRUInfo : ${clanNRUInfoPath}")
//    Spark.writeToAvro(clanNRUInfoDF, country, clanNRUInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} clanUVInfo : ${clanUVInfoPath}")
//    Spark.writeToAvro(clanUVInfoDF, country, clanUVInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} nonClanUserInfo : ${nonClanUserInfoPath}")
//    Spark.writeToAvro(nonClanUserInfoDF, country, nonClanUserInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} nonClanNRUInfo : ${nonClanNRUInfoPath}")
//    Spark.writeToAvro(nonClanNRUInfoDF, country, nonClanNRUInfoPath, "overwrite")
//
//    logger.info(s"save ${projectName} ${sourceName} nonClanUVInfo : ${nonClanUVInfoPath}")
//    Spark.writeToAvro(nonClanUVInfoDF, country, nonClanUVInfoPath, "overwrite")
//
//  }
//}
