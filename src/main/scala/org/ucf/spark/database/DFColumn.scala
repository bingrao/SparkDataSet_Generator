package org.ucf.spark
package database

case class DFColumn(colName:String, colType:String) {
  def getName = this.colName
  def getType = colType
}
