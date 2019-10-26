package org.ucf.spark
package dataframe

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select._

class DataFrame extends common.EnrichedTrees {


  def parse(sql:String):String = {
    val statement = CCJSqlParserUtil.parse(sql)
    val dataframe = new StringBuilder()
    if (statement.isInstanceOf[Select]) {
      statement.asInstanceOf[Select].genCode(dataframe)
    }
    else
      logger.info("Only support select statement: " + statement)
    return dataframe.toString()
  }
}
