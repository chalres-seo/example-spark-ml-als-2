package com.example.utils

import com.example.config.HdfsConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import scala.collection.JavaConverters._
import scala.collection.JavaConversions.
_
import scala.util.{Failure, Success, Try}

object Hdfs extends LazyLogging {
  private val defaultHdfsConf = HdfsConfig.hdfsConfigurtion

  @throws[Exception]
  private def getHdfs: FileSystem = {
    this.getHdfs(defaultHdfsConf)
  }

  private def getHdfs(hdfsConf: Configuration) = {
    try {
      FileSystem.get(hdfsConf)
    } catch {
      case e:Exception => {
        logger.error("failed get hdfs.", e)
        throw e
      }
    }
  }

  @throws[Exception]
  def getList(path:String, recursive:Boolean): Vector[LocatedFileStatus] = {
    logger.info(s"get hdfs list. path: $path, recursive: $recursive")

    this.hdfsWithCleanup(_.listFiles(new Path(path), recursive)) match {
      case Success(list) => convertRemoteIteratorToScalaIterator(list).toVector
      case Failure(e) =>
        logger.error(e.getMessage, e)
        Vector.empty[LocatedFileStatus]
    }
  }

//  private implicit def convertRemoteIteratorToScalaIterator[T](underlying: RemoteIterator[T]): Iterator[T] = {
    private def convertRemoteIteratorToScalaIterator[T](underlying: RemoteIterator[T]): Iterator[T] = {
      case class wrapper(underlying: RemoteIterator[T]) extends Iterator[T] {
        override def hasNext: Boolean = underlying.hasNext

        override def next: T = underlying.next
      }
      wrapper(underlying)
    }

  private def hdfsWithCleanup[R](fn: FileSystem => R): Try[R] = {
    AppUtils.cleanupResource(this.getHdfs, logger)(_.close())(fn)
  }
}
