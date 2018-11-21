package utils

import org.apache.log4j.Logger
import org.apache.log4j.LogManager

/**
  *  Created by wangjx
  *  日志记录接口，跟spark本身的日志分开记录
  */
object LoggerUtil extends Serializable {
  @transient lazy val logger:Logger = LogManager.getLogger(getClass.getName.stripSuffix("$"))

  /**
    * 输出debug日志
    * @param message，日志消息对象
    * */
  def debug(message: Object): Unit = {
    logger.debug(message)
  }

  /**
    * 输出info日志
    * @param message，日志消息对象
    * */
  def file(message: Object): Unit = {
    logger.info(message)
  }

  /**
    * 输出warn日志
    * @param message，日志消息对象
    * */
  def warn(message: Object): Unit = {
    logger.warn(message)
  }

  /**
    * 输出error日志
    * @param message，日志消息对象
    * */
  def error(message: Object): Unit = {
    logger.error(message)
  }

  /**
    * 输出info日志, 由于本处打印的日志无法与spark日志分开, 故使用error等级输出属于info范畴的日志. 此处使用logInfo间接调用error, 为
    * 以后可能实现日志分开做准备
    * @param message，日志消息对象
    * */
  def logInfo(message: Object): Unit = {
    error(message)
    //logger.info(message)
  }
}

