package org.ucf.spark

import net.sf.jsqlparser.expression.{BinaryExpression, Expression, Function}
import net.sf.jsqlparser.schema.Column

/**
  * @author 
  */
object ScalaApp extends common.Common {
  def main(args: Array[String]): Unit = {

//    def getExpressionString(expression: Expression):String  = {
//      if (expression == null) return regEmpty
//      expression match {
//        case column: Column => {
//          val colName = column.getColumnName
//          if(column.getTable != null) {
//            val tableName = tableList.getOrElse(column.getTable.getName, column.getTable.getName)
//            s"${tableName}(" + "\"" + colName + "\"" + ")"
//          } else {
//            "\"" + colName + "\""
//          }
//        } // City or t1.name
//        case func:Function => {
//          if(func.getParameters != null ){
//            val params = func.getParameters.getExpressions.toList
//              .map(getExpressionString _).mkString(",")
//            func.getName + "(" + params + ")"
//          } else {
//            func.toString
//          }
//        } // max(a)
//        case binaryExpr:BinaryExpression => {
//          val leftString = getExpressionString(binaryExpr.getLeftExpression)
//          val rightString = getExpressionString(binaryExpr.getRightExpression)
//          val op = binaryExpr.getStringExpression
//          s"${leftString} ${op} ${rightString}"
//        } // t1.name = t2.name
//        case _ => {
//          expression.toString
//        }
//      }
//    }
    implement.SpiderFileReader.genSparkDataFrame()
    implement.WikiSQLFileReader.genSparkDataFrame()
  }
}