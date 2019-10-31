package org.ucf.spark
package codegen

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.Table
import net.sf.jsqlparser.statement.select.{Join, SelectItem}
import org.ucf.spark.utils.PropertiesLoader

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConversions._
class Context(configPath:String = "src/main/resources/application.conf") extends common.Common {
  val parameters = new PropertiesLoader(configPath)
  val selectList = new ListBuffer[SelectItem]() // All select list
  def addSelect(selectItem: SelectItem) = selectList.+=(selectItem)
  def addSelect(selectItemList: List[SelectItem]) = selectList.addAll(selectItemList)
  private var joinList = new ListBuffer[Join]() // All Join list
  def addJoin(join: Join) = joinList.+=(join)
  val df = new StringBuilder()

  // A record (alias -> name)
  var tableList:mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
  def addTable(table:Table):Unit = if (table != null){
    if(table.getAlias != null) // (alias --> Name)
      tableList +=(table.getAlias.getName -> table.getName)
    else
      tableList +=(table.getName -> table.getName)
  }
  def getTable(table:Table) = this.tableList.getOrElse(table.getName, table.getName)
  val exceptionList = new ListBuffer[Throwable]()
  def addException(throwable: Throwable) = exceptionList.+=(throwable)

  /**
    *  So JSQLParser (v3.0) has debugs when parse a sql which contains following keywords:
    *   START
    *   MATCH
    *   CHARACTER
    *   SHOW
    *   NOT
    *   ORDER BY COUNT(*)  >=  5
    * @param sql
    * @return
    */
  def isSQLValidate(sql: String): Boolean = {
    Try {
      CCJSqlParserUtil.parse(sql)
    } match {
      case Success(_) => true
      case Failure(ex) => {
        addException(ex)
        false
      }
    }
  }



}
