package org.ucf.spark
package codegen

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import scala.util.{Failure, Success, Try}

class Generator(context: Context = new Context()) extends common.EnrichedTrees {
  def genCodeFromSQLString(sql:String, ctx: Context = context) = {
    Try {
      CCJSqlParserUtil.parse(sql).genCode(ctx)
    } match {
      case Success(_) =>
      case Failure(ex) => throw new Exception(s"Exception while parsing following sql \n $sql " +
        s" \n Cause of exception is \n ${ex.getCause}")
    }
  }
  def getContext = this.context
  def preGenerator(sql:String, ctx: Context = context) = {
    logger.debug("******************************************\n\n")
    logger.debug("INPUT SQL: " + sql)
    // check current SQL if it is a valid sql statement
    if(!ctx.isSQLValidate(sql)){
      logger.error(s"The input is not a valid SQL statement: $sql")
      ctx.disableSupport
    }
  }
  def postGenerator(sql:String, ctx: Context = context) = {
    // Print out debug info
    if(logger.isDebugEnabled)
      ctx.getDebugInfo()

    //After generator, the context need to be reset for next run
    ctx.reset
  }
  def run(sql:String, ctx: Context = context):String = {
    preGenerator(sql)
    genCodeFromSQLString(sql,ctx)
    if(!ctx.isSupport) {
      ctx.enableSupport
      //      logger.info(s"${unSupportNotice} INPUT SQL: ${sql} ${unSupportNotice}")
      //      logger.info(s"${unSupportNotice} OUTPUT DataFrame: ${dataframe.toString()} ${unSupportNotice}\n")
      ctx.append(unSupportNotice) // doest not remove this statement, will used in spider filter
    }
    val reg = ctx.getSparkDataFrame
    logger.debug("OUTPUT DataFrame: " + reg)
    postGenerator(sql)
    reg
  }
}