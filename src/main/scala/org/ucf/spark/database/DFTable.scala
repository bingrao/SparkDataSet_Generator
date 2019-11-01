package org.ucf.spark
package database

import scala.collection.mutable.ListBuffer

case class DFTable(tableName:String){
  private val columnDefs = new ListBuffer[DFColumn]
  private var alias:String = EmptyString
  def addColumn(col:DFColumn) = columnDefs.+=(col)
  def getName = this.tableName

  def setAlias(rename: String) = this.alias = rename
  def getAlias() = this.alias
}
