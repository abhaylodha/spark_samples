package my.spark.common_utils

import org.slf4j.LoggerFactory

trait Logger {
  val logger = LoggerFactory.getLogger(this.getClass)
  def info(msg: String) = logger.info(msg)
  def warn(msg: String) = logger.warn(msg)
  def debug(msg: String) = logger.debug(msg)
  def error(msg: String) = logger.error(msg)
}
