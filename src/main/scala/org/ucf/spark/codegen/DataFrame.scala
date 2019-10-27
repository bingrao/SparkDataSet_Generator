package org.ucf.spark
package codegen

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select._

object DataFrame extends common.EnrichedTrees {
  def codeGen(sql:String):String = {

    logger.debug("******************************************\n\n")
    logger.debug("INPUT SQL: " + sql)

    val statement = CCJSqlParserUtil.parse(sql)
    val dataframe = new StringBuilder()
    if (statement.isInstanceOf[Select]) {
      statement.asInstanceOf[Select].genCode(dataframe)
    }
    else
      logger.info("Only support select statement: " + statement)


    if(unSupport) {
      this.unSupport = false
      logger.info(s"${unSupportNotice} INPUT SQL: ${sql} ${unSupportNotice}")
      logger.info(s"${unSupportNotice} OUTPUT DataFrame: ${dataframe.toString()} ${unSupportNotice}\n")
      dataframe.append(unSupportNotice) // doest not remove this statement, will used in spider filter
    }

    logger.debug("OUTPUT DataFrame: " + dataframe.toString())
    return dataframe.toString()
  }
}
