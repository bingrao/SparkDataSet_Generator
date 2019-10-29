package org.ucf.spark
package common

import net.sf.jsqlparser.statement.select._
import net.sf.jsqlparser.statement.values._
import net.sf.jsqlparser.schema._
import net.sf.jsqlparser.statement.select.Join
import net.sf.jsqlparser.expression._
import net.sf.jsqlparser.expression.operators.relational._
import net.sf.jsqlparser.expression.operators.arithmetic._
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.util.cnfexpression._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

trait EnrichedTrees extends Common {

  /*********************************************************************************************************/
  /*******************************   Global Varibles to Record info ****************************************/
  /*********************************************************************************************************/
  var tableList:mutable.HashMap[String, String] = // A record (alias -> name)
    new mutable.HashMap[String, String]()
  var joinList = new ListBuffer[Join]() // All Join list
  var selectList = new ListBuffer[SelectItem]() // All select list
  var unSupport:Boolean = false
  var currentData:String = EmptyString
  val exceptionList = new ListBuffer[Throwable]
  /*********************************************************************************************************/
  /*****************************   Implicit class for JSQLparser Node *************************************/
  /*********************************************************************************************************/
  implicit class genSelect(select:Select){
    def genCode(df:mutable.StringBuilder):String  = {
      if (!unSupport) {
        select.getSelectBody.genCode(df)
      }
      getDebugInfo()
      resetEnvironment() // Reset environment for next run
      EmptyString
    }
  }
  implicit class genSelectBody(body:SelectBody) {
    def genCode(df:mutable.StringBuilder):String = {
      if (!unSupport) {
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
            logger.error("Select Body Error: " + body)
          }
        }
      }
      EmptyString
    }
  }
  implicit class genPlainSelect(body:PlainSelect){
    def genCode(df:mutable.StringBuilder):String  = {
      if (!unSupport) {
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
        val groupItems = if (body.getGroupBy != null) genCodeGroupBy(body.getGroupBy, df) else EmptyString
        val aggCols = if (body.getSelectItems != null) genCodeSelect(body.getSelectItems.toList,df, groupItems) else EmptyString
        if (body.getHaving != null) genCodeHaving(body.getHaving, df, groupItems)
        if (body.getOrderByElements != null) genCodeOrderBy(body.getOrderByElements.toList, df, aggCols)
        if (body.getDistinct != null) genCodeDistinct(body.getDistinct, df)
        if (body.getLimit != null) genCodeLimit(body.getLimit, df)
      }
      EmptyString
    }
  }
  implicit class genJoin(join:Join) {
    def genCode(df:mutable.StringBuilder):String = {
      if (!unSupport) {
        val right = getTableName(join.getRightItem.asInstanceOf[Table])
        val condition = join.getOnExpression.getString()
        val joinStatement = if (join.isSimple && join.isOuter) {
          s"join($right, $condition, outer)"
        } else if (join.isSimple) {
          s"join($right, $condition, ) "
        } else {
          if (join.isRight) {
            s"join($right, $condition, right)"
          } else if (join.isNatural) {
            s"join($right, $condition, inner)"
          } else if (join.isFull) {
            s"join($right, $condition, full)"
          } else if (join.isLeft) {
            s"join($right, $condition, left)"
          } else if (join.isCross) {
            s"join($right, $condition, cross)"
          }

          if (join.isOuter) {
            s"join($right, $condition, outer)"
          } else if (join.isInner) {
            s"join($right, $condition, inner)"
          } else if (join.isSemi) {
            s"join($right, $condition, left_semi)"
          }

          if (!join.isStraight) {
            s"join($right, $condition, inner)"
          } else {
            s"join($right, $condition, STRAIGHT_JOIN)"
          }
        }
        df.append("." + joinStatement)
      }
      EmptyString
    }
  }
  implicit class genExpression(expr: Expression) {
    def genCode(df:mutable.StringBuilder):String = {
      if (!unSupport) {
        df.append(this.getString(expr))
      }
      EmptyString
    }
    def getString(expression: Expression = expr):String  = {
      var subSelect: Boolean = false
      if ((expression != null) && (!unSupport))  {
        expression match {
          case operation@(_: JsonExpression |
                          _: NumericBind |
                          _: ArrayExpression |
                          _: ExistsExpression |
                          _: InExpression |
                          _: KeepExpression |
                          _: CaseExpression |
                          _: OracleHint |
                          _: AnalyticExpression |
                          _: IsBooleanExpression |
                          _: NotExpression |
                          _: ValueListExpression |
                          _: RowConstructor |
                          //                          _:DoubleValue |
                          _: CastExpression |
                          _: UserVariable |
                          _: CollateExpression |
                          _: MySQLGroupConcat |
                          _: AllComparisonExpression |
                          _: ExtractExpression |
                          _: FullTextSearch |
                          _: OracleHierarchicalExpression |
                          _: IsNullExpression |
                          _: AnyComparisonExpression |
                          _: SignedExpression |
                          //                          _:NullValue |
                          _: SubSelect |
                          _: JdbcNamedParameter |
                          _: IntervalExpression |
                          _: WhenClause |
                          //                          _:LongValue |
                          _: JdbcParameter |
                          //                          _:TimeValue |
                          _: MultipleExpression |
                          //                          _:TimestampValue |
                          //                          _:TimeKeyExpression |
                          //                          _:DateValue |
                          _: NextValExpression |
                          //                          _:StringValue |
                          _: Parenthesis |
                          _: DateTimeLiteralExpression) => {
            unSupport = true
            val message = s"Unsupport OP in condition [${operation.toString}]:[${operation.getClass.getTypeName}]"
            exceptionList += new Throwable(message)
            message
          }
          case column: Column => {
            val colName = column.getColumnName
            if (column.getTable != null) {
              val tableName = tableList.getOrElse(column.getTable.getName, column.getTable.getName)
              s"$tableName(" + "\"" + colName + "\"" + ")"
            } else {
              "\"" + colName + "\""
            }
          } // City or t1.name
          case func: Function => {
            if (func.getParameters != null) {
              val params = func.getParameters.getExpressions.toList
                .map(_.getString()).mkString(",")
              func.getName + "(" + params + ")"
            } else { // count(*)
              func.getName + "(\"*\")"
            }
          } // max(a)
          case binaryExpr: BinaryExpression => {
            binaryExpr match {
              case operation@(_: LikeExpression |
                              _: SimilarToExpression |
                              _: RegExpMySQLOperator |
                              _: OldOracleJoinBinaryExpression |
                              _: RegExpMatchOperator |
                              _: IntegerDivision |
                              _: BitwiseLeftShift |
                              _: BitwiseRightShift |
                              _: JsonOperator) => {
                unSupport = true
                val message = s"Unsupport OP in condition [${operation.toString}]:[${operation.getClass.getTypeName}]"
                exceptionList += new Throwable(message)
                message
              }

              case _ => {
                /**
                  * There is a bug here, Because a string is parsed as a column.
                  * For example "asin" > "a", where "asin" is a column name, "a"
                  * is just a string. Here the generate code will be col("asin") > col("a")
                  * As we can see, it is wrong, the corret answer should be col("asin") > "a"
                  * So metadata of the corresponding table is used to fixe out this issue.
                  */
                val leftString = getColumnName(binaryExpr.getLeftExpression)
                val rightString = binaryExpr.getRightExpression.getString()
                val op = binaryExpr.getStringExpression.toUpperCase
                op match {
                  case "=" => s"$leftString === $rightString"
                  case "!=" => s"$leftString =!= $rightString"
                  case "AND" => s"$leftString && $rightString"
                  case "OR" => s"$leftString || $rightString"
                  case _ => s"$leftString ${op} $rightString"
                }
              }
            }
          } // t1.name = t2.name
          case between: Between => {
            if (between.getLeftExpression.isInstanceOf[SubSelect]) subSelect = true
            if (between.getBetweenExpressionStart.isInstanceOf[SubSelect]) subSelect = true
            if (between.getBetweenExpressionEnd.isInstanceOf[SubSelect]) subSelect = true

            val left = getColumnName(between.getLeftExpression)
            val start = between.getBetweenExpressionStart.getString()
            val end = between.getBetweenExpressionEnd.getString()
            s"$left >= $start and $left =< $end"
          }
          case _ => {
            logger.error(s"There is no expression type: ${expression.getClass.getTypeName}")
            expression.toString
          }
        }
      } else EmptyString
    }
    def isFuncOrBinary(expression: Expression = expr) = expression match {
      case _:Function => true
      case _:BinaryExpression => true
      case _ => false
    }
  }
  implicit class genSetOperationList(body: SetOperationList){
    def genCode(df:mutable.StringBuilder):String = {
      if (!unSupport) {
        val selects = body.getSelects.toList
        val operations = body.getOperations.toList
        val brackets = body.getBrackets

        for (i <- selects.indices) {
          if (i != 0) {
            val op = operations.get(i - 1).toString.toLowerCase
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

        if (body.getOrderByElements != null) genCodeOrderBy(body.getOrderByElements.toList,df, EmptyString)

        if (body.getLimit != null) genCodeLimit(body.getLimit, df)

        if (body.getOffset != null) df.append(body.getOffset.toString)

        if (body.getFetch != null) df.append(body.getFetch.toString)
      }
      EmptyString
    }
  }

  private def genCodeFrom(from:FromItem ,df:mutable.StringBuilder):mutable.StringBuilder  = {
    if (!unSupport){
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
          unSupport = true
          throw new UnsupportedOperationException("Not supported yet.")
        }
        case valuelist: ValuesList => {
          unSupport = true
          //TODO
          throw new UnsupportedOperationException("Not supported yet.")
        }
        case tableFunc: TableFunction => {
          unSupport = true
          //TODO
          throw new UnsupportedOperationException("Not supported yet.")
        }
        case _ => {
          unSupport = true
          //TODO
          throw new UnsupportedOperationException("Not supported yet.")
        }
      }
    }
    df
  }
  private def genCodeJoins(joins: List[Join] ,df:mutable.StringBuilder) = {
    if (!unSupport) {
      joins.foreach(join => {
        addTable(join.getRightItem.asInstanceOf[Table])
        joinList += join
        join.genCode(df)
      })
    }
    df
  }
  private def genCodeWhere(where:Expression,df:mutable.StringBuilder)  = {
    if (!unSupport) {
      val whereString = where.getString()
      df.append(s".filter($whereString)")
    }
    df
  }
  private def genCodeGroupBy(groupByElement: GroupByElement,df:mutable.StringBuilder)  = {
    var groupExpressionsString = EmptyString
    if (!unSupport) {
      groupExpressionsString = groupByElement
        .getGroupByExpressions
        .map( expression => {
          getColumnName(expression)
        })
        .mkString(",")
      df.append(s".groupBy($groupExpressionsString)")
    }
    groupExpressionsString
  }
  private def genCodeSelect(selectItems: List[SelectItem],df:mutable.StringBuilder,groupBy:String):String  = {
    var aggCols = EmptyString
    if (!unSupport) {
      var selectAgg: Boolean = false
      var selectColumn: Boolean = false
      var allAlias: Boolean = false
      val selectString = selectItems.map(
        select => { select match {
          case sExp: SelectExpressionItem => {
            sExp.getExpression match {
              case func:Function => {
                selectAgg = true
                //                val sExpStringArray = func.getString().split("[()]")
                //                val sExprString = if (sExpStringArray.last.contains("*")){
                //                  allAlias = true
                //                  if(sExpStringArray.size > 1)
                //                    sExpStringArray.head + "(\"all.*\")"
                //                  else
                //                    "(\"all.*\")"
                //                } else {
                //                  if(sExpStringArray.size > 1)
                //                    sExpStringArray.head + "(" + sExpStringArray.last + ")"
                //                  else
                //                    sExpStringArray.last
                //                }
                val sExprString = func.getString()
                if(sExp.getAlias != null) {
                  sExprString + " as \"" + sExp.getAlias.getName +"\""
                } else {
                  sExprString
                }
              }
              case column:Column => {
                selectColumn = true
                if(sExp.getAlias != null) {
                  getColumnName(column) + ".as(\"" + sExp.getAlias.getName +"\")"
                } else {
                  getColumnName(column)
                }
              }
              case _ => {
                getColumnName(sExp.getExpression)
              }
            }
          }
          case aTcolumns: AllTableColumns => {
            aTcolumns.toString
          }
          case aColumns: AllColumns => { // count(*)
            "col(\"" + aColumns.toString + "\")"
          }
          case _ => {
            logger.error("select item is wrong" + select)
            select.toString
          }
        }}).mkString(",")
      /*
      *  Check following condition that not support by current
      *  1. select min(asin), price from product
      *  2. select asin, price from product group by brand
      * */
      if (selectAgg && selectColumn) {
        this.unSupport = true
        df.append("Current Version does not support to sellect column and agg")
        exceptionList += new Throwable(df.toString())
        return aggCols
      } else if ((!groupBy.isEmpty) && selectColumn){
        this.unSupport = true
        df.append("Current Version does not support groupBy operation without agg funcs in select")
        exceptionList += new Throwable(df.toString())
        return aggCols
      }

      /**
        * For select count(*) from product, there is a asterisk, so we need to add an alias "all" on [[product]] table
        */
      //      if(allAlias) {
      //        val dfSize = df.size
      //        var tableName = EmptyString
      //        var tableIndex = Int.MinValue
      //        tableList.foreach{
      //          case (alias, name) => {
      //            val index = df.indexOf(name)
      //            if((index != -1) &&(index > tableIndex)){
      //              tableIndex =  index
      //              tableName = name
      //            }
      //          }
      //        }
      //        if(tableName != EmptyString) {
      //          df.insert(tableIndex + tableName.length, ".alias(\"all\")")
      //        }
      //      }
      if (!groupBy.isEmpty || selectAgg) {
        df.append(s".agg($selectString)")
        aggCols = selectString
      } else
        df.append(s".select($selectString)")
    }
    return aggCols
  }
  private def genCodeHaving(havingExpr: Expression,df:mutable.StringBuilder, groupBy:String) = {
    if(groupBy.isEmpty){
      unSupport = true
      df.append("Need groupBy operation if you want to use having statement")
      exceptionList += new Throwable(df.toString())
      df
    } else {
      val havingString = havingExpr.getString()
      df.append(s".filter($havingString)")
    }
  }
  private def genCodeOrderBy(orderByElement: List[OrderByElement] ,df:mutable.StringBuilder, aggCols: String):mutable.StringBuilder  = {
    if (!unSupport) {
      var isFuncOrBinary:Boolean = false
      if (aggCols.isEmpty) {
        val eleString = orderByElement.map(ele => {
          if(ele.getExpression.isFuncOrBinary()) {
            isFuncOrBinary = true
          }
          val expStringList = ele.getExpression.getString().split("[()]") // column name will be in the last pos
          val order = if (!ele.isAsc) "desc"
          else if (ele.isAscDescPresent) "asc"
          else EmptyString

          if(order != EmptyString) { // User specify order way (asc or desc) explicitly
            order + "(" + expStringList.last + ")"
          } else {
            getColumnName(ele.getExpression)
          }
        }).mkString(",")
        df.append(s".orderBy($eleString)")
        if(isFuncOrBinary){
          this.unSupport = true
          df.append("Current Version does not support to order by with a func or binary operation")
          exceptionList += new Throwable(df.toString())
          return df
        }
      } else {
        this.unSupport = true
        df.append("Current Version does not support to order by from an agg selection without group by")
        exceptionList += new Throwable(df.toString())
        return df
      }
    }
    df
  }
  private def genCodeDistinct(distinct: Distinct ,df:mutable.StringBuilder)  = {
    if (!unSupport) {
      df.append(s".distinct")
    }
    df
  }
  private def genCodeLimit(limit: Limit ,df:mutable.StringBuilder)  = {
    if (!unSupport) {
      val nums = limit.getRowCount.getString()
      df.append(s".limit($nums)")
    }
    df
  }

  /*********************************************************************************************************/
  /****************************************   Helper Functions *********************************************/
  /*********************************************************************************************************/
  private def getTableName(table: Table) = table.getName
  private def addTable(table:Table):Unit = if (table != null){
    if(table.getAlias != null) // (alias --> Name)
      tableList +=(table.getAlias.getName -> table.getName)
    else
      tableList +=(table.getName -> table.getName)
  }
  private def resetEnvironment(): Unit ={
    this.tableList.clear()
    this.joinList.clear()
    this.selectList.clear()
    this.exceptionList.clear()
  }
  private def getColumnName(expression: Expression):String = {
    val name = expression.getString()
    if(expression.isInstanceOf[Column])
      if(name.split("[()]").length > 1)
        name
      else
        "col(" + name + ")"
    else
      name
  }

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
        exceptionList.add(ex)
        false
      }
    }
  }

  def getDebugInfo(): Unit = {
    logger.debug("[Table<Alias, Name>] " + tableList.mkString(","))
    logger.debug("[Join] " + joinList.mkString(","))
    logger.debug("[Select] " + selectList.mkString(","))
    exceptionList.foreach(throwable => logger.debug(throwable.getCause))
  }
}
