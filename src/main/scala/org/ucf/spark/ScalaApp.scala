package org.ucf.spark

/**
  * @author 
  */
object ScalaApp extends common.Common {
  def main(args: Array[String]): Unit = {
    logger.info("Start Converst SQL to Spark DataFrame ...")
    implement.SpiderFileReader.parser()
    logger.info("Conversation is over!!")
  }
}