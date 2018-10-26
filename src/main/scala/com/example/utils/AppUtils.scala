package com.example.utils

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

import com.typesafe.scalalogging.{LazyLogging, Logger}

import scala.util.{Failure, Success, Try}

object AppUtils extends LazyLogging {
  def writeLocalFile(savePathString: String, records: Vector[String], append: Boolean): Unit = {
    val savePath = Paths.get(savePathString)

    if(Files.notExists(savePath)) {
      Files.createDirectories(savePath.getParent)
    }

    val fileWriter = if (append) {
      Files.newBufferedWriter(savePath,
        StandardCharsets.UTF_8,
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND)
    } else {
      Files.newBufferedWriter(savePath,
        StandardCharsets.UTF_8,
        StandardOpenOption.CREATE)
    }

    records.foreach { record =>
      Try {
        fileWriter.write(record)
        fileWriter.newLine()
      } match {
        case Success(_) =>
          logger.debug(s"write record: $record")
        case Failure(e) =>
          logger.error(e.getMessage)
          logger.error(record)
      }
    }

    logger.debug("file flushing and close.")
    fileWriter.flush()
    fileWriter.close()
  }

  def cleanupResource[A, B](resource: A, logger: Logger)(cleanup: A => Unit)(fn: A => B): Try[B] = {
    try {
      Success(fn(resource))
    } catch {
      case e: Exception => Failure(e)
    } finally {
      try {
        if (resource != null) {
          cleanup(resource)
        }
      } catch {
          case e: Exception =>
            logger.error("failed clean up.")
            logger.error(e.getMessage, e)
        }
      }
  }
}