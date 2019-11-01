package org.ucf.spark
package common

import java.util.List
import net.sf.jsqlparser.schema.{Database, Table}
import net.sf.jsqlparser.statement.create.table.ColumnDefinition
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

trait Common extends Logger{
  /**
    *
    * @param expectedDf  : Expected DataFrame Code
    * @param convertedDf : Converted DataFrame Code
    * @return
    */
  def compareDataFrame(expectedDf: String, convertedDf: String): Boolean = {
    expectedDf.toUpperCase.replaceAll("\\s", EmptyString) ==
      convertedDf.toUpperCase.replaceAll("\\s", EmptyString)
  }
  implicit class addAPIColumnDefinition(columnDefinition: ColumnDefinition) {
    def printPretty(sb:mutable.StringBuilder, numsIntent:Int) = {
      sb.append(this.getIndent(numsIntent))
        .append(s"${columnDefinition.getColumnName}:${columnDefinition.getColDataType.toString}, ")
    }
  }
  case class TableWrapper(table: Table) {
    private var idx = 0
    def getIndex = this.idx
    def setIndex(index:Int) = this.idx = index
    def this(tableName:String) = this(new Table(tableName))

    private val columnDefinitions = new ListBuffer[ColumnDefinition]
    def addColumnDefinitions(list:List[ColumnDefinition]) =
      this.columnDefinitions.addAll(list)
    def addColumnDefinition(colDef:ColumnDefinition) =
      this.columnDefinitions.add(colDef)

    def getJTable = this.table
    def getName = table.getName


    def isColumnExist(colName:String) = {
      val columns = columnDefinitions.filter(_.getColumnName.equals(colName))
      if(columns.size != 0) true else false
    }


    def printPretty(sb:mutable.StringBuilder, numsIntent:Int) = {
      sb.append("\n").append(this.getIndent(numsIntent)).append("Table: " + getName).append("{")
      columnDefinitions.foreach(_.printPretty(sb, 0))
      sb.update(sb.size - 2, '}')
    }
  }
  case class DatabaseWrapper(dbName: String) {
    private val jdb = new Database(dbName)
    def getJDB = this.jdb
    def getDatabaseName = this.dbName

    private val tableList = new mutable.HashMap[String, TableWrapper]()
    def getOrElseUpdate(table:TableWrapper) =
      tableList.getOrElseUpdate(table.getName, table)
    def getOrElseUpdate(tableName:String) =
      tableList.getOrElseUpdate(tableName, new TableWrapper(tableName))

    def isContainTable(tableName:String) = tableList.contains(tableName)
    def getTable(tableName:String) = tableList.getOrElse(tableName, null)

    def printPretty(sb:mutable.StringBuilder, numsIntent:Int) = {
      sb.append("\n" + this.getIndent(numsIntent)).append("Database: " + dbName)
      tableList.foreach{  case (tableName, table) => {
        table.printPretty(sb, numsIntent + 1)
      }}
    }
  }
}

