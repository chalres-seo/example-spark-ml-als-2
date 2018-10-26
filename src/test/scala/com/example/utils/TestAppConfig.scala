package com.example.utils

import java.io.File

import com.example.config.{AppConfig, HdfsConfig}
import com.typesafe.config.ConfigFactory
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

class TestAppConfig {
  @Test
  def testHdfsConfig(): Unit = {
//    println(HdfsConfig.uri)
//    println()
//    println(HdfsConfig.hdfsUri)
//    println(HdfsConfig.getHdfsRootPath)
//    println(HdfsConfig.getHdfsMLPath)
//    println(HdfsConfig.getHdfsSourceList)
//    println(HdfsConfig.getHdfsSourceList.map(HdfsConfig.getHdfsSourcePath))
//
//    println()
//
//    val a = ConfigFactory.parseFile(new File("conf/spark.conf")).resolve()
//
//    println(a.getConfig("spark-config").entrySet().toVector)
  }
}
