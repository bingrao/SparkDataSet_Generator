package org.ucf.spark
package codegen

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select._

object DataFrame extends common.EnrichedTrees {
  def codeGen(sql:String):String = {
    logger.debug("\n\n******************************************")
    logger.debug("INPUT SQL: " + sql)
    val statement = CCJSqlParserUtil.parse(sql)
    val dataframe = new StringBuilder()
    if (statement.isInstanceOf[Select]) {
      statement.asInstanceOf[Select].genCode(dataframe)
    }
    else
      logger.info("Only support select statement: " + statement)

    logger.debug("OUTPUT DataFrame: " + dataframe.toString())

    return dataframe.toString()
  }
}
