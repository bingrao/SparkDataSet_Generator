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
      if(unSupport) {
        this.unSupport = false
        //      logger.info(s"${unSupportNotice} INPUT SQL: ${sql} ${unSupportNotice}")
        //      logger.info(s"${unSupportNotice} OUTPUT DataFrame: ${dataframe.toString()} ${unSupportNotice}\n")
        ctx.df.append(unSupportNotice) // doest not remove this statement, will used in spider filter
      }
      logger.debug("OUTPUT DataFrame: " + ctx.df.toString())
    } match {
      case Success(_) => return ctx.df.toString()
      case Failure(ex) => throw new Exception(s"Exception while parsing following sql \n $sql " +
        s" \n Cause of exception is \n ${ex.getCause}")
    }
  }
}