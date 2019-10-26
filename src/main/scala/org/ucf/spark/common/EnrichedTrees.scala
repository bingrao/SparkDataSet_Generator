package org.ucf.spark
package common

import net.sf.jsqlparser.statement.select._
import net.sf.jsqlparser.statement.values._
import net.sf.jsqlparser.schema._
import net.sf.jsqlparser.statement.select.Join
import net.sf.jsqlparser.expression._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

trait EnrichedTrees extends Common {

  val regEmpty:String = ""
  var tableList:mutable.HashMap[String, String] = new mutable.HashMap[String, String]() // (alias -> name)
  var joinList = new ListBuffer[Join]()
  var selectList = new ListBuffer[SelectItem]()

  implicit class genSelect(select:Select){
    def genCode(df:mutable.StringBuilder):String  = select.getSelectBody.genCode(df)
  }
  implicit class genSelectBody(body:SelectBody) {
    def genCode(df:mutable.StringBuilder):String = {
      body match {
        case pSelect:PlainSelect => {
          pSelect.genCode(df)
        }
        case sSelect:SetOperationList => {
          sSelect.genCode(df)

        }
        case wItem:WithItem => {
          //TODO
          throw new UnsupportedOperationException("Not supported yet.")
        }
        case vStatement:ValuesStatement => {
          //TODO
          throw new UnsupportedOperationException("Not supported yet.")
        }
        case _ => {
          logger.info("Select Body Error: " + body)
        }
      }
      regEmpty
    }
  }

  implicit class genPlainSelect(body:PlainSelect){
    def genCode(df:mutable.StringBuilder):String  = {
      logger.debug("PlainSelect:" + body)
      selectList.addAll(body.getSelectItems.toList)
      if (body.getFromItem != null) genCodeFrom(body.getFromItem, df)
      if (body.getJoins != null) genCodeJoins(body.getJoins.toList, df)
      if (body.getWhere != null) genCodeWhere(body.getWhere, df)
      if (body.getGroupBy != null) genCodeGroupBy(body.getGroupBy, df)
      if (body.getOrderByElements != null) genCodeOrderBy(body.getOrderByElements.toList, df)
      if (body.getSelectItems != null) genCodeSelect(body.getSelectItems.toList, df)
      if (body.getDistinct != null) genCodeDistinct(body.getDistinct, df)
      if (body.getLimit != null) genCodeLimit(body.getLimit, df)

      tableList.foreach(list => logger.debug(list.toString(), true))
      joinList.foreach(list => logger.debug(list.toString(), true))
      selectList.foreach(list => logger.debug(list.toString(), true))

      regEmpty
    }
    def genCodeFrom(from:FromItem ,df:mutable.StringBuilder):String  = {
      from match {
        case subjoin:SubJoin => {
          val leftTable = subjoin.getLeft.asInstanceOf[Table]
          addTable(leftTable)
          df.append(getTableName(leftTable))
          val joins = subjoin.getJoinList.toList
          joins.foreach(join => {
            addTable(join.getRightItem.asInstanceOf[Table])
            joinList += join
            join.genCode(df)
          })
        }
        case table:Table => {
          addTable(table)
          val tableName = getTableName(table)
          df.append(tableName)
        }
        case parFrom:ParenthesisFromItem =>{}
        case subselect:SubSelect =>{
          subselect.getSelectBody.genCode(df)
        }
        case lsubselect:LateralSubSelect =>{}
        case valuelist:ValuesList =>{}
        case tableFunc:TableFunction =>{}
        case _ => {

        }
      }
      regEmpty
    }
    def genCodeJoins(joins: List[Join] ,df:mutable.StringBuilder):String = {
      joins.foreach(join => {
        addTable(join.getRightItem.asInstanceOf[Table])
        joinList += join
        join.genCode(df)
      })
      regEmpty
    }
    def genCodeWhere(where:Expression,df:mutable.StringBuilder):String  = {
      val whereString = getExpressionString(where)
      df.append(s".filter(${whereString})")
      regEmpty
    }
    def genCodeSelect(selectItems: List[SelectItem],df:mutable.StringBuilder):String  = {
      val selectString = selectItems.map(select => {
        select match {
          case sExp:SelectExpressionItem => {
            getExpressionString(sExp.getExpression)
          }
          case aTcolumns: AllTableColumns => {
            aTcolumns.toString
          }
          case aColumns: AllColumns => {
            aColumns.toString
          }
          case _ => {
            logger.info("select item is wrong" + select)
            select.toString
          }
        }
      }).mkString(",")

      df.append(s".select(${selectString})")

      regEmpty
    }
    def genCodeGroupBy(groupByElement: GroupByElement,df:mutable.StringBuilder):String  = {
      //      people.filter("age > 30")
      //        .join(department, people("deptId") === department("id"))
      //        .groupBy(department("name"), people("gender"))
      //        .agg(avg(people("salary")), max(people("age")))
      val aggSelectList = selectList.filter(selectItem => {
        selectItem.isInstanceOf[SelectExpressionItem] &&
          selectItem.asInstanceOf[SelectExpressionItem].getExpression.isInstanceOf[Function]
      }).map(selectItem => {
        selectItem.asInstanceOf[SelectExpressionItem].toString
      }).mkString(",")

      val groupExpressionsString = groupByElement
        .getGroupByExpressions
        .map(getExpressionString _)
        .mkString(",")
      df.append(s".groupBy(${groupExpressionsString}).agg(${aggSelectList})")

      regEmpty
    }
    def genCodeOrderBy(orderByElement: List[OrderByElement] ,df:mutable.StringBuilder):String  = {
      val eleString = orderByElement.map(ele => {
        getExpressionString(ele.getExpression)
      }).mkString(",")
      df.append(s".orderBy($eleString)")
      regEmpty
    }
    def genCodeDistinct(distinct: Distinct ,df:mutable.StringBuilder):String  = {
      df.append(s".distinct")
      regEmpty
    }
    def genCodeLimit(limit: Limit ,df:mutable.StringBuilder):String  = {
      val nums = getExpressionString(limit.getRowCount)
      df.append(s".takeAsList($nums)")
      regEmpty
    }
  }
  implicit class genJoin(join:Join) {
    def genCode(df:mutable.StringBuilder):String = {
      val right = getTableName(join.getRightItem.asInstanceOf[Table])
      val condition = getExpressionString(join.getOnExpression)
      val joinStatement = if (join.isSimple() && join.isOuter()) {
        s"join(${right}, ${condition}, outer)"
      } else if (join.isSimple()) {
        s"join(${right}, ${condition}, ) "
      } else {

        if (join.isRight()) {
          s"join(${right}, ${condition}, right)"
          " "
        } else if (join.isNatural()) {
          s"join(${right}, ${condition}, inner)"
          " "
        } else if (join.isFull()) {
          s"join(${right}, ${condition}, full)"
        } else if (join.isLeft()) {
          s"join(${right}, ${condition}, left)"
          " "
        } else if (join.isCross()) {
          s"join(${right}, ${condition}, cross)"
        }

        if (join.isOuter()) {
          s"join(${right}, ${condition}, outer)"
        } else if (join.isInner()) {
          s"join(${right}, ${condition}, inner)"
        } else if (join.isSemi()) {
          s"join(${right}, ${condition}, left_semi)"
        }

        if (!join.isStraight()) {
          s"join(${right}, ${condition}, inner)"
        } else {
          s"join(${right}, ${condition}, STRAIGHT_JOIN)"
        }
      }
      df.append("."+ joinStatement)
      regEmpty
    }
  }

  implicit class genExpression(expression: Expression) {
    def genCode(df:mutable.StringBuilder):String = {
      df.append(getExpressionString(expression))
      regEmpty
    }
  }

  implicit class genSetOperationList(body: SetOperationList){
    def genCode(df:mutable.StringBuilder):String = {
      logger.debug("SetOperationList:" + body)
      val selects = body.getSelects.toList
      val operations = body.getOperations.toList
      val orderByElements = body.getOrderByElements
      val limit = body.getLimit
      selects.foreach(body => body.genCode(df))
      regEmpty
    }
  }

  private def getTableName(table: Table) = table.getName
  private def getExpressionString(expression: Expression):String  = {
    expression match {
      case column: Column => {
        val colName = column.getColumnName
        if(column.getTable != null) {
          val tableName = tableList.getOrElse(column.getTable.getName, column.getTable.getName)
          s"${tableName}.${colName}"
        } else {
          s"${colName}"
        }
      } // City or t1.name
      case func:Function => {
        func.toString
      } // max(a)
      case binaryExpr:BinaryExpression => {
        val leftString = getExpressionString(binaryExpr.getLeftExpression)
        val rightString = getExpressionString(binaryExpr.getRightExpression)
        val op = binaryExpr.getStringExpression
        s"${leftString} ${op} ${rightString}"
      } // t1.name = t2.name
      case _ => {
        expression.toString
      }
    }
  }
  private def addTable(table:Table):Unit = if (table != null){
    if(table.getAlias != null) // (alias --> Name)
      tableList +=(table.getAlias.getName -> table.getName)
    else
      tableList +=(table.getName -> table.getName)
  }
}
