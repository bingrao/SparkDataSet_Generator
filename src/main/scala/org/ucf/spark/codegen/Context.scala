package org.ucf.spark
package codegen

import java.io.{File, FileFilter}

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.{Database, Table}
import net.sf.jsqlparser.statement.select.{Join, SelectItem}
import org.ucf.spark.utils.PropertiesLoader

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConversions._
import scala.io.Source

class Context(configPath:String = "src/main/resources/application.conf")
  extends common.Common {

  val parameters = new PropertiesLoader(configPath)

  val selectList = new ListBuffer[SelectItem]() // All select list
  def addSelect(selectItem: SelectItem) = selectList.+=(selectItem)
  def addSelect(selectItemList: List[SelectItem]) = selectList.addAll(selectItemList)

  private var joinList = new ListBuffer[Join]() // All Join list
  def addJoin(join: Join) = joinList.+=(join)

  private val df = new StringBuilder()

  def getSparkDataFrame = df.toString()
  def append(content:Any) = df.append(content)

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
  def addException(exp:String) = exceptionList.+= (new Throwable(exp))
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
        addException(s"[SQL Statement $sql]\n" + ex.toString)
        false
      }
    }
  }

  private var support:Boolean = true
  def isSupport = this.support
  def disableSupport = this.support = false
  def enableSupport = this.support = true

  def getTableName(table: Table) = table.getName

  def getDebugInfo(): Unit = {
    logger.debug("[Table<Alias, Name>] " + tableList.mkString(","))
    logger.debug("[Join] " + joinList.mkString(","))
    logger.debug("[Select] " + selectList.mkString(","))
    exceptionList.foreach(throwable => logger.debug(throwable.getCause))
  }

  def reset = {
    this.tableList.clear()
    this.joinList.clear()
    this.df.clear()
    this.selectList.clear()
    this.exceptionList.clear()
    this.enableSupport
  }

  private val databases = mutable.HashMap[String, DatabaseWrapper]()  //<database name --> DB>
  def getOrElseUpdateDB(dbName:String) = databases.getOrElseUpdate(dbName, new DatabaseWrapper(dbName))

  def getAllDB = this.databases
  def allDBToString = {
    val sb = new mutable.StringBuilder()
    getAllDB.foreach{case (_, db) => {
      db.printPretty(sb,0)
    }}
    sb.toString()
  }

  private var currentDB:DatabaseWrapper = _
  def getCurrentDB = this.currentDB
  def setCurretnDB(dbName:String) = {
    this.currentDB = getOrElseUpdateDB(dbName)
    this.currentDB
  }
}
