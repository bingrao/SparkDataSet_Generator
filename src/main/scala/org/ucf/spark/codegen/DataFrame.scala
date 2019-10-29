package org.ucf.spark
package codegen

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select._
import scala.util.{Failure, Success, Try}

class DataFrame extends common.EnrichedTrees {
  def codeGen(sql:String):String = {
    logger.debug("******************************************\n\n")
    logger.debug("INPUT SQL: " + sql)
    val dataframe = new StringBuilder()
    Try {
      val statement = CCJSqlParserUtil.parse(sql)
      if (statement.isInstanceOf[Select]) {
        statement.asInstanceOf[Select].genCode(dataframe)
      }
      else
        logger.info("Only support select statement: " + statement)

      if(unSupport) {
        this.unSupport = false
        //      logger.info(s"${unSupportNotice} INPUT SQL: ${sql} ${unSupportNotice}")
        //      logger.info(s"${unSupportNotice} OUTPUT DataFrame: ${dataframe.toString()} ${unSupportNotice}\n")
        dataframe.append(unSupportNotice) // doest not remove this statement, will used in spider filter
      }

      logger.debug("OUTPUT DataFrame: " + dataframe.toString())
    } match {
      case Success(_) => return dataframe.toString()
      case Failure(ex) => throw new Exception(s"Exception while parsing following sql \n $sql " +
        s" \n Cause of exception is \n ${ex.getCause}")
    }
  }
}
object DataFrame extends DataFrame
