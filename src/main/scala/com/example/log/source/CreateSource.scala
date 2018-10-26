//package com.example.log.source
//
//import com.typesafe.scalalogging.LazyLogging
//import org.joda.time.DateTime
//
///**
//  * create pb all resouce daily-data
//  */
//object CreateSource extends LazyLogging {
//
//  /**
//    * create all resouce day data
//    *
//    * @param country checked country name
//    * @param dateTime checked date time
//    */
//  def createDay(country:String, dateTime:DateTime) = {
//    UserList.writeDay(country, dateTime)
//    UserInfo.writeDay(country, dateTime)
//    SaleList.writeDay(country, dateTime)
//  }
//
//  /**
//    * create all resouce month data
//    *
//    * @param country checked country name
//    * @param dateTime checked date time
//    */
//  def createMonth(country:String, dateTime:DateTime) = {
//    UserList.writeMonth(country, dateTime)
//    UserInfo.writeMonth(country, dateTime)
//    SaleList.writeMonth(country, dateTime)
//  }
//
//  /**
//    * create all resouce year data
//    *
//    * @param country checked country name
//    * @param dateTime checked date time
//    */
//  def createYear(country:String, dateTime:DateTime) = {
//    UserList.writeYear(country, dateTime)
//    UserInfo.writeYear(country, dateTime)
//    SaleList.writeYear(country, dateTime)
//  }
//}
