package org.ucf.spark

import net.sf.jsqlparser.expression.{BinaryExpression, Expression, Function}
import net.sf.jsqlparser.schema.Column

/**
  * @author 
  */
object ScalaApp extends common.Common {
  def main(args: Array[String]): Unit = {
    implement.SpiderFileReader.genSparkDataFrame()
    implement.WikiSQLFileReader.genSparkDataFrame()
  }
}