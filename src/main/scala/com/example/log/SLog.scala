//package com.example.log
//
//import com.typesafe.scalalogging.LazyLogging
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.types.StructType
//import org.joda.time.DateTime
//
///**
//  * Created by charles on 12/15/17.
//  */
//private[log] class SLog(projectName:String) extends LazyLogging {
//  private val gzlogHdfsRootPath = Util.getHdfsGZLogPath(projectName)
//
//  def getGZLogDF(country:String, logid:String, year:String, month:String, day:String): DataFrame = {
//    this.getGZLogDF(gzlogHdfsRootPath +s"/${country}/${year}/month=${month}/logid=${logid}/*${year}${month}${day}*")
//  }
//
//  def getGZLogDF(country:String, logid:String, year:String, month:String): DataFrame ={
//    this.getGZLogDF(gzlogHdfsRootPath +s"/${country}/${year}/month=${month}/logid=${logid}/*")
//  }
//
//  def getGZLogDF(country:String, logid:String, year:String): DataFrame = {
//    this.getGZLogDF(gzlogHdfsRootPath +s"/${country}/${year}/month=*/logid=${logid}/*")
//  }
//
//  def getGZLogDayDF(country:String, logid:String, date:DateTime): DataFrame = {
//    this.getGZLogDF(country, logid, date.toString("yyyy"), date.toString("MM"), date.toString("dd"))
//  }
//
//  def getGZLogMonthDF(country:String, logid:String, date:DateTime): DataFrame = {
//    this.getGZLogDF(country, logid, date.toString("yyyy"), date.toString("MM"))
//  }
//
//  def getGZLogYearDF(country:String, logid:String, date:DateTime): DataFrame = {
//    this.getGZLogDF(country, logid, date.toString("yyyy"))
//  }
//
//  private def getGZLogDF(path:String): DataFrame = {
//    logger.info(s"get ${projectName} gzlog df from : ${path}")
//    val spark = Spark.getSparkSession()
//    val gzlogSchema: StructType = Util.getGZLogSchema(projectName)
//
//    spark.read
//      .option("delimiter", "\t")
//      .option("header", "false")
//      .schema(gzlogSchema)
//      .csv(path)
//  }
//}
