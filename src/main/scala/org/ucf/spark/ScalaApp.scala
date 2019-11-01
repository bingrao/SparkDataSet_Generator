package org.ucf.spark
import implement._
/**
  * @author 
  */
object ScalaApp extends common.Common {
  def main(args: Array[String]): Unit = {
    SpiderFileReader.genSparkDataFrame()
    WikiSQLFileReader.genSparkDataFrame()
  }
}