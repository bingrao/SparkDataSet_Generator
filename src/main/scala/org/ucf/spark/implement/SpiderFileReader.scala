package org.ucf.spark
package implement

import java.io.FileWriter
import java.util.UUID.randomUUID

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select._
import net.sf.jsqlparser.statement.values._
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization.write
import org.json4s.{DefaultFormats, JObject, JString}

import scala.io.Source

/**
  * https://www.hackingnote.com/en/scala/json4s
  */
object SpiderFileReader extends common.Common {
  def jsonParser(path:String = "input/train_spider.json") = {
    implicit val formats = DefaultFormats
    val source = Source.fromFile(path)
  }
  def sqlParser(sql:String) = {
    val statement = CCJSqlParserUtil.parse(sql)

    if (statement.isInstanceOf[Select]) {
      logger.info("Select Statement: " + statement)
      val body = statement.asInstanceOf[Select].getSelectBody

      if (body.isInstanceOf[PlainSelect]) {
        logger.info("Select Body PlainSelect: " + body)
        val plainBody = body.asInstanceOf[SetOperationList]


      } else if (body.isInstanceOf[SetOperationList]) {
        logger.info("Select Body SetOperationList: " + body)
        val setBody = body.asInstanceOf[SetOperationList]

        setBody.getOperations
        setBody.getSelects
        setBody.getOrderByElements
        setBody.getLimit
        setBody.getBrackets



      }else if (body.isInstanceOf[WithItem]){


        logger.info("Select Body WithItem: " + body)


      } else if (body.isInstanceOf[ValuesStatement]){


        logger.info("Select Body ValuesStatement: " + body)


      } else{

        logger.info("Select Body Error: " + body)
      }
    }
    else
      logger.info("Only support select statement: " + statement)

  }
}
