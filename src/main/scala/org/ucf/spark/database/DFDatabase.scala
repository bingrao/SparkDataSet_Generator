package org.ucf.spark
package database

import scala.collection.mutable

case class DFDatabase(dbName:String) {
  private val tableList = new mutable.HashMap[String, DFTable]()
  def getTable(tableName:String) = tableList.getOrElseUpdate(tableName, new DFTable(tableName))
  def isTableExists(tableName:String) = this.tableList.contains(tableName)
}
