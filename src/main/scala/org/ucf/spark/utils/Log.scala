package org.ucf.spark.utils
import  org.apache.log4j.Logger

class Log(name:String){
  private val logger = Logger.getLogger(name)

  def info(message:Any,prefix:Boolean = true) =
    if (prefix) logger.info(message) else println(message)
  def warn(message:Any,prefix:Boolean = true) =
    if (prefix) logger.warn(message) else println(message)
  def debug(message:Any,prefix:Boolean = true) =
    if (prefix) logger.debug(message) else println(message)
  def error(message:Any,prefix:Boolean = true) =
    if (prefix) logger.debug(message) else println(message)

  def isDebugEnabled = logger.isDebugEnabled
}