package org.ucf.spark

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import org.ucf.spark.codegen.Context

import scala.util.{Failure, Success, Try}

class DFTestBase extends common.EnrichedTrees with common.Common {
  def codeGen(sql:String):String = {
    logger.debug("******************************************\n\n")
    logger.debug("INPUT SQL: " + sql)
    val ctx = new Context()
    Try {
      CCJSqlParserUtil.parse(sql).genCode(ctx)
      if(!ctx.isSupport) {
        ctx.enableSupport
        //      logger.info(s"${unSupportNotice} INPUT SQL: ${sql} ${unSupportNotice}")
        //      logger.info(s"${unSupportNotice} OUTPUT DataFrame: ${dataframe.toString()} ${unSupportNotice}\n")
        ctx.append(unSupportNotice) // doest not remove this statement, will used in spider filter
      }
      logger.debug("OUTPUT DataFrame: " + ctx.getSparkDataFrame)
    } match {
      case Success(_) => {
        val reg = ctx.getSparkDataFrame
        ctx.reset
        return reg
      }
      case Failure(ex) => throw new Exception(s"Exception while parsing following sql \n $sql " +
        s" \n Cause of exception is \n ${ex.getCause}")
    }
  }
}