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

  /*********************************************************************************************************/
  /*******************************   Global Varibles to Record info ****************************************/
  /*********************************************************************************************************/
  val regEmpty:String = ""  // recursive function return value for gencode func in implicit class
  var tableList:mutable.HashMap[String, String] = // A record (alias -> name)
    new mutable.HashMap[String, String]()
  var joinList = new ListBuffer[Join]() // All Join list
  var selectList = new ListBuffer[SelectItem]() // All select list
  var unSupport:Boolean = false

  /*********************************************************************************************************/
  /*****************************   Implicit class for JSQLparser Node *************************************/
  /*********************************************************************************************************/
  implicit class genSelect(select:Select){
    def genCode(df:mutable.StringBuilder):String  = {
      if (unSupport == false) {
        select.getSelectBody.genCode(df)
      }
      regEmpty
    }
  }
  implicit class genSelectBody(body:SelectBody) {
    def genCode(df:mutable.StringBuilder):String = {
      if (unSupport == false) {
        body match {
          case pSelect: PlainSelect => {
            pSelect.genCode(df)
          }
          case sSelect: SetOperationList => {
            sSelect.genCode(df)
          }
          case wItem: WithItem => {
            //TODO
            throw new UnsupportedOperationException("Not supported yet.")
          }
          case vStatement: ValuesStatement => {
            //TODO
            throw new UnsupportedOperationException("Not supported yet.")
          }
          case _ => {
            logger.info("Select Body Error: " + body)
          }
        }
        resetEnvironment() // Reset environment for next run
      }
      regEmpty
    }
  }
  implicit class genPlainSelect(body:PlainSelect){
    def genCode(df:mutable.StringBuilder):String  = {
      if (unSupport == false) {
        selectList.addAll(body.getSelectItems.toList)

        /**
          * MYSQL Execution order
          * https://qxf2.com/blog/mysql-query-execution/
          * 1. FROM clause
          * 2. WHERE clause
          * 3. GROUP BY clause
          * 4. HAVING clause
          * 5. SELECT clause
          * 5. ORDER BY clause
          * However, HAVING and GROUP BY clauses can come after SELECT depending on
          * the order it is specified in the query.
          */
        if (body.getFromItem != null) genCodeFrom(body.getFromItem, df)
        if (body.getJoins != null) genCodeJoins(body.getJoins.toList, df)
        if (body.getWhere != null) genCodeWhere(body.getWhere, df)
        val groupItems = if (body.getGroupBy != null) genCodeGroupBy(body.getGroupBy, df) else regEmpty
        if (body.getHaving != null) {
          unSupport = true
          df.append("Not support having statement so far, still n process")
          return regEmpty
        }
        val aggCols = if (body.getSelectItems != null) genCodeSelect(body.getSelectItems.toList,df, groupItems) else regEmpty
        if (body.getOrderByElements != null) genCodeOrderBy(body.getOrderByElements.toList, df, aggCols)
        if (body.getDistinct != null) genCodeDistinct(body.getDistinct, df)
        if (body.getLimit != null) genCodeLimit(body.getLimit, df)

        logger.debug("[Table<Alias, Name>] " + tableList.mkString(","))
        logger.debug("[Join] " + joinList.mkString(","))
        logger.debug("[Select] " + selectList.mkString(","))
      }
      regEmpty
    }
  }
  implicit class genJoin(join:Join) {
    def genCode(df:mutable.StringBuilder):String = {
      if (unSupport == false) {
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
        df.append("." + joinStatement)
      }
      regEmpty
    }
  }
  implicit class genExpression(expr: Expression) {
    def genCode(df:mutable.StringBuilder):String = {
      if (unSupport == false) {
        df.append(this.getString(expr))
      }
      regEmpty
    }
    def getString(expression: Expression = expr):String  = {
      if (expression == null) return regEmpty
      expression match {
        case column: Column => {
          val colName = column.getColumnName
          if(column.getTable != null) {
            val tableName = tableList.getOrElse(column.getTable.getName, column.getTable.getName)
            s"${tableName}(" + "\"" + colName + "\"" + ")"
          } else {
            "\"" + colName + "\""
          }
        } // City or t1.name
        case func:Function => {
          if(func.getParameters != null ){
            val params = func.getParameters.getExpressions.toList
              .map(getExpressionString _).mkString(",")
            func.getName + "(" + params + ")"
          } else {
            func.toString
          }
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
  }
  implicit class genSetOperationList(body: SetOperationList){
    def genCode(df:mutable.StringBuilder):String = {
      if (unSupport == false) {
        val selects = body.getSelects.toList
        val operations = body.getOperations.toList
        val brackets = body.getBrackets

        for (i <- 0 to (selects.size - 1)) {
          if (i != 0) {
            val op = operations.get(i - 1).toString().toLowerCase
            if (op.equals("minus")) throw new UnsupportedOperationException("Unsupport Operation")
            df.append(" ").append(op).append(" ")
          }
          if (brackets == null || brackets.get(i)) {
            df.append("(")
            selects.get(i).genCode(df)
            df.append(")")
          } else {
            selects.get(i).genCode(df)
          }
        }

        if (body.getOrderByElements != null) genCodeOrderBy(body.getOrderByElements.toList,df, regEmpty)

        if (body.getLimit != null) genCodeLimit(body.getLimit, df)

        if (body.getOffset != null) df.append(body.getOffset.toString())

        if (body.getFetch != null) df.append(body.getFetch.toString())
      }
      regEmpty
    }
  }


  private def genCodeFrom(from:FromItem ,df:mutable.StringBuilder):mutable.StringBuilder  = {
    if (unSupport == false){
      from match {
        case subjoin: SubJoin => {
          val leftTable = subjoin.getLeft.asInstanceOf[Table]
          addTable(leftTable)
          df.append(getTableName(leftTable))
          val joins = subjoin.getJoinList.toList
          joins.foreach(join => {
            addTable(join.getRightItem.asInstanceOf[Table])
            joinList += join
            join.genCode(df)
          })
          df
        }
        case table: Table => {
          addTable(table)
          val tableName = getTableName(table)
          df.append(tableName)
        }
        case parFrom: ParenthesisFromItem => {}
        case subselect: SubSelect => {
          subselect.getSelectBody.genCode(df)
        }
        case lsubselect: LateralSubSelect => {
          //TODO
          throw new UnsupportedOperationException("Not supported yet.")
        }
        case valuelist: ValuesList => {
          //TODO
          throw new UnsupportedOperationException("Not supported yet.")
        }
        case tableFunc: TableFunction => {
          //TODO
          throw new UnsupportedOperationException("Not supported yet.")
        }
        case _ => {
          //TODO
          throw new UnsupportedOperationException("Not supported yet.")
        }
      }
    }
    df
  }
  private def genCodeJoins(joins: List[Join] ,df:mutable.StringBuilder) = {
    if (unSupport == false) {
      joins.foreach(join => {
        addTable(join.getRightItem.asInstanceOf[Table])
        joinList += join
        join.genCode(df)
      })
    }
    df
  }
  private def genCodeWhere(where:Expression,df:mutable.StringBuilder)  = {
    if (unSupport == false) {
      val whereString = getExpressionString(where)
      df.append(s".filter(${whereString})")
      //    df.append(".where(\"" + where + "\")")
    }
    df
  }
  private def genCodeSelect(selectItems: List[SelectItem],df:mutable.StringBuilder,groupBy:String):String  = {
    var aggCols = regEmpty
    if (unSupport == false) {
      var haveAgg: Boolean = false
      var havaColumn: Boolean = false
      val selectString = selectItems.map(
        select => { select match {
          case sExp: SelectExpressionItem => {
            if (sExp.getExpression.isInstanceOf[Function]){ // max(price)
              haveAgg = true
              if(sExp.getAlias != null) {
                getExpressionString(sExp.getExpression) + " as \"" + sExp.getAlias.getName +"\""
              } else {
                getExpressionString(sExp.getExpression)
              }
            } else if (sExp.getExpression.isInstanceOf[Column]) { // t1.a, a
              havaColumn = true
              val column = sExp.getExpression.asInstanceOf[Column]
              if(sExp.getAlias != null) {
                if (column.getTable != null) // review("asin").as("id")
                  getExpressionString(sExp.getExpression) + ".as(\"" + sExp.getAlias.getName +"\")"
                else // col("asin").as("id")
                  s"col(" + getExpressionString(sExp.getExpression) + ").as(\"" + sExp.getAlias.getName +"\")"
              } else {
                if (column.getTable != null)
                  getExpressionString(sExp.getExpression) // review("asin")
                else
                  s"col(" + getExpressionString(sExp.getExpression) + ")" // col("asin")
              }
            } else {
              getExpressionString(sExp.getExpression)
            }
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
        }}).mkString(",")
      /*
      *  Check following condition that not support by current
      *  1. select min(asin), price from product
      *  2. select asin, price from product group by brand
      * */
      if (haveAgg && havaColumn) {
        this.unSupport = true
        df.append("Current Version does not support to sellect column and agg")
        //              throw new UnsupportedOperationException("Current Version does not support to sellect column and agg")
        return aggCols
      } else if ((!groupBy.isEmpty) && (havaColumn)){
        this.unSupport = true
        df.append("Current Version does not support groupBy operation without agg funcs in select")
        //              throw new UnsupportedOperationException("Current Version does not support to sellect column and agg")
        return aggCols
      }

      if (!groupBy.isEmpty) {
//        df.append(s".agg(${selectString}).drop(${groupBy})")
        df.append(s".agg(${selectString})")
      } else if (haveAgg == true) {
//        df.append(s".groupBy().agg(${selectString})")
        df.append(s".agg(${selectString})")
        aggCols = selectString
      } else
        df.append(s".select(${selectString})")
    }
    return aggCols
  }
  private def genCodeGroupBy(groupByElement: GroupByElement,df:mutable.StringBuilder)  = {
    var groupExpressionsString = regEmpty
    if (unSupport == false) {
      //      people.filter("age > 30")
      //        .join(department, people("deptId") === department("id"))
      //        .groupBy(department("name"), people("gender"))
      //        .agg(avg(people("salary")), max(people("age")))
      //    val aggSelectList = selectList.filter(selectItem => {
      //      selectItem.isInstanceOf[SelectExpressionItem] &&
      //        selectItem.asInstanceOf[SelectExpressionItem].getExpression.isInstanceOf[Function]
      //    }).map(selectItem => {
      //      selectItem.asInstanceOf[SelectExpressionItem].toString
      //    }).mkString(",")

      groupExpressionsString = groupByElement
        .getGroupByExpressions
        .map( expression => {
          val expStringList = expression.getString().split("[()]") // column name will be in the last pos
          if (expStringList.size > 1) // which means already include a table, e.g product("asin")
            expStringList.head + "(" + expStringList.last + ")" // col("asin")
          else
            "col(" + expStringList.last + ")" // col("asin")
        })
        .mkString(",")
      //    df.append(s".groupBy(${groupExpressionsString}).agg(${aggSelectList})")
      df.append(s".groupBy(${groupExpressionsString})")
    }
    groupExpressionsString
  }
  private def genCodeOrderBy(orderByElement: List[OrderByElement] ,df:mutable.StringBuilder, aggCols: String):mutable.StringBuilder  = {
    if (unSupport == false) {
      if (aggCols.isEmpty) {
        val eleString = orderByElement.map(ele => {
          val expStringList = ele.getExpression.getString().split("[()]") // column name will be in the last pos
          val order = if (!ele.isAsc) "desc"
          else if (ele.isAscDescPresent) "asc"
          else regEmpty

          if(order != regEmpty) { // User specify order way (asc or desc) explicitly
            order + "(" + expStringList.last + ")"
          } else {
            if (expStringList.size > 1)
              expStringList.head + "(" + expStringList.last + ")"
            else
              expStringList.mkString
          }
        }).mkString(",")
        df.append(s".orderBy($eleString)")
      } else {
        this.unSupport = true
        df.append("Current Version does not support to order by from an agg selection without group by")
        //              throw new UnsupportedOperationException("Current Version does not support to sellect column and agg")
        return df
      }
    }
    df
  }
  private def genCodeDistinct(distinct: Distinct ,df:mutable.StringBuilder)  = {
    if (unSupport == false) {
      df.append(s".distinct")
    }
    df
  }
  private def genCodeLimit(limit: Limit ,df:mutable.StringBuilder)  = {
    if (unSupport == false) {
      val nums = getExpressionString(limit.getRowCount)
      df.append(s".limit($nums)")
    }
    df
  }


  /*********************************************************************************************************/
  /****************************************   Helper Functions *********************************************/
  /*********************************************************************************************************/
  private def getTableName(table: Table) = table.getName
  private def getExpressionString(expression: Expression):String  = {
    if (expression == null) return regEmpty
    expression match {
      case column: Column => {
        val colName = column.getColumnName
        if(column.getTable != null) {
          val tableName = tableList.getOrElse(column.getTable.getName, column.getTable.getName)
          s"${tableName}(" + "\"" + colName + "\"" + ")"
        } else {
          "\"" + colName + "\""
        }
      } // City or t1.name
      case func:Function => {
        if(func.getParameters != null ){
          val params = func.getParameters.getExpressions.toList
            .map(getExpressionString _).mkString(",")
          func.getName + "(" + params + ")"
        } else {
          func.toString
        }
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
  private def resetEnvironment(): Unit ={
    this.tableList = new mutable.HashMap[String, String]()
    this.joinList = new ListBuffer[Join]()
    this.selectList = new ListBuffer[SelectItem]()
  }
}
