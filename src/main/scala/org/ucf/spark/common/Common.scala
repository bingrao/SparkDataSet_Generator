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
        .append(s"${columnDefinition.getColumnName}:${columnDefinition.getColDataType.toString}")
    }
  }

  case class TableWrapper(table: Table) {

    private var jtable:Table = table
    def setJTable(table: Table) = this.jtable = table
    def getJTable = this.jtable
    def getName = table.getName


    private val columnDefinitions = new ListBuffer[ColumnDefinition]

    def setColumnDefinitions(list:List[ColumnDefinition]) =
      this.columnDefinitions.addAll(list)

    def getColumnDefinitions = this.columnDefinitions

    def printPretty(sb:mutable.StringBuilder, numsIntent:Int) = {
      sb.append("\n").append(this.getIndent(numsIntent)).append("Table: " + getName).append("{")
      columnDefinitions.foreach( coldef => {
        coldef.printPretty(sb, 0)
        if (columnDefinitions.last != coldef)
          sb.append(",")
      })
      sb.append("}")
    }
  }
  case class DatabaseWrapper(dbName: String) {
    private val jdb = new Database(dbName)
    def getJDB = this.jdb
    def getDatabaseName = this.dbName

    private val dbtableList = new mutable.HashMap[String, TableWrapper]()
    def addTable(table:TableWrapper):Unit = if (table != null){
      dbtableList += (table.getName -> table)
    }
    def getTableSize = dbtableList.size

    def isTableExists(tableName:String) = this.dbtableList.contains(tableName)
    def printPretty(sb:mutable.StringBuilder, numsIntent:Int) = {
      sb.append("\n" + this.getIndent(numsIntent)).append("Database: " + dbName)
      dbtableList.foreach{  case (tableName, table) => {
        table.printPretty(sb, numsIntent + 1)
      }}
    }
  }


}

