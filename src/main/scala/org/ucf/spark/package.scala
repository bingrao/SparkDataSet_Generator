package org.ucf

import org.apache.log4j.Logger

package object spark extends Enumeration{
  final val unSupportNotice = "[***UNSUPPORT***]"
  final val EmptyString:String = ""  // recursive function return value for gencode func in implicit class

  // input file format
  val JSON = Value("Json")
  val JSONL = Value("Jsonl")
  val NORMAL = Value("Normal")

  final val DFObject = "SparkDataFrame"

  // add log functions to all Scala Objects
  implicit class AddLogger(any:AnyRef) {
    private val logger = Logger.getLogger(DFObject)
    def logInfo(message:Any,prefix:Boolean = true) =
      if (prefix) logger.info(message) else println(message)
    def logWarn(message:Any,prefix:Boolean = true) =
      if (prefix) logger.warn(message) else println(message)
    def logDebug(message:Any,prefix:Boolean = true) =
      if (prefix) logger.debug(message) else println(message)
    def logError(message:Any,prefix:Boolean = true) =
      if (prefix) logger.debug(message) else println(message)
    def isLogDebugEnabled = logger.isDebugEnabled
  }
}
