package org.ucf.spark
package codegen

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import scala.util.{Failure, Success, Try}

class Generator(context: Context = new Context()) extends common.EnrichedTrees {
  def genCodeFromSQLString(sql:String, ctx: Context = context):String = {
    ctx.logger.debug("******************************************\n\n")
    ctx.logger.debug("INPUT SQL: " + sql)
    Try {
      CCJSqlParserUtil.parse(sql).genCode(ctx)
      if(!ctx.isSupport) {
        ctx.enableSupport
        //      logger.info(s"${unSupportNotice} INPUT SQL: ${sql} ${unSupportNotice}")
        //      logger.info(s"${unSupportNotice} OUTPUT DataFrame: ${dataframe.toString()} ${unSupportNotice}\n")
        ctx.append(unSupportNotice) // doest not remove this statement, will used in spider filter
      }
      ctx.logger.debug("OUTPUT DataFrame: " + ctx.getSparkDataFrame)
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
  def getContext = this.context
}