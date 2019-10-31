package org.ucf.spark
package common

import net.sf.jsqlparser.statement.select._
import net.sf.jsqlparser.statement.values._
import net.sf.jsqlparser.schema._
import net.sf.jsqlparser.statement.select.Join
import net.sf.jsqlparser.expression._
import net.sf.jsqlparser.expression.operators.relational._
import net.sf.jsqlparser.expression.operators.arithmetic._
import net.sf.jsqlparser.util.cnfexpression._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import net.sf.jsqlparser.statement._
import database._
import codegen.Context

trait EnrichedTrees extends DFDatabaseBuilder{

  /*********************************************************************************************************/
  /*******************************   Global Varibles to Record info ****************************************/
  /*********************************************************************************************************/
  var unSupport:Boolean = false


  /*********************************************************************************************************/
  /*****************************   Implicit class for JSQLparser Node *************************************/
  /*********************************************************************************************************/
  implicit class genStatement(statement: Statement) {
    def genCode(ctx:Context):String = if (!unSupport) {
      val df = ctx.df
      statement match {
          case sts @ (_:net.sf.jsqlparser.statement.drop.Drop |
                      _:net.sf.jsqlparser.statement.truncate.Truncate |
                      _:DeclareStatement |
                      _:net.sf.jsqlparser.statement.alter.Alter |
                      _:net.sf.jsqlparser.statement.SetStatement |
                      _:net.sf.jsqlparser.statement.merge.Merge  |
                      _:net.sf.jsqlparser.statement.update.Update |
                      _:net.sf.jsqlparser.statement.upsert.Upsert |
                      _:net.sf.jsqlparser.statement.Commit |
                      _:net.sf.jsqlparser.statement.ShowStatement |
                      _:net.sf.jsqlparser.statement.ShowColumnsStatement |
                      _:net.sf.jsqlparser.statement.values.ValuesStatement |
                      _:net.sf.jsqlparser.statement.create.index.CreateIndex |
//                      _:create.table.CreateTable |
                      _:net.sf.jsqlparser.statement.create.view.AlterView |
                      _:net.sf.jsqlparser.statement.create.view.CreateView |
                      _:net.sf.jsqlparser.statement.comment.Comment |
//                      _: Block |
                      _:net.sf.jsqlparser.statement.execute.Execute |
//                      _:select.Select |
                      _:net.sf.jsqlparser.statement.ExplainStatement |
                      _:net.sf.jsqlparser.statement.replace.Replace |
                      _:net.sf.jsqlparser.statement.DeclareStatement |
                      _:net.sf.jsqlparser.statement.insert.Insert |
                      _:net.sf.jsqlparser.statement.delete.Delete |
                      _:net.sf.jsqlparser.statement.UseStatement) =>{
            EmptyString
          }
          case block: Block => {
            block.genCode(ctx)
          }
          case cratetable: create.table.CreateTable =>{
            cratetable.genCode(ctx)
          }
          case sel: select.Select =>{
            sel.genCode(ctx)
          }
          case _ => {
            EmptyString
          }
        }
    } else EmptyString
  }
  implicit class genCreateTable(createtable:create.table.CreateTable) {
    def genCode(ctx:Context):String = if (!unSupport) {
      val df = ctx.df
      val table = createtable.getTable
      val columnDef = createtable.getColumnDefinitions
      val index = createtable.getIndexes


      EmptyString
    } else EmptyString
  }
  implicit class genBlock(block: Block) {
    def genCode(ctx:Context):String = if (!unSupport) {
      EmptyString
    } else EmptyString
  }
  implicit class genSelect(select:Select){
    def genCode(ctx:Context):String  = if (!unSupport) {
      select.getSelectBody.genCode(ctx)
    } else EmptyString
  }


  implicit class genSelectBody(body:SelectBody) {
    def genCode(ctx:Context):String = {
      if (!unSupport) {
        body match {
          case pSelect: PlainSelect => {
            pSelect.genCode(ctx)
          }
          case sSelect: SetOperationList => {
            sSelect.genCode(ctx)
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
            ctx.logger.error("Select Body Error: " + body)
          }
        }
      }
      EmptyString
    }
  }
  implicit class genPlainSelect(body:PlainSelect){
    def genCode(ctx:Context):String  = {
      if (!unSupport) {
        ctx.addSelect(body.getSelectItems.toList)
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
        if (body.getFromItem != null) genCodeFrom(body.getFromItem, ctx)
        if (body.getJoins != null) genCodeJoins(body.getJoins.toList, ctx)
        if (body.getWhere != null) genCodeWhere(body.getWhere, ctx)
        val groupItems = if (body.getGroupBy != null) genCodeGroupBy(body.getGroupBy, ctx) else EmptyString
        val aggCols = if (body.getSelectItems != null) genCodeSelect(body.getSelectItems.toList,ctx, groupItems) else EmptyString
        if (body.getHaving != null) genCodeHaving(body.getHaving, ctx, groupItems)
        if (body.getOrderByElements != null) genCodeOrderBy(body.getOrderByElements.toList, ctx, aggCols)
        if (body.getDistinct != null) genCodeDistinct(body.getDistinct, ctx)
        if (body.getLimit != null) genCodeLimit(body.getLimit, ctx)
      }
      EmptyString
    }
  }
  implicit class genJoin(join:Join) {
    def genCode(ctx:Context):String = {
      if (!unSupport) {
        val right = getTableName(join.getRightItem.asInstanceOf[Table])
        val condition = join.getOnExpression.getString(ctx)
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
        ctx.df.append("." + joinStatement)
      }
      EmptyString
    }
  }
  implicit class genExpression(expr: Expression) {
    def genCode(ctx:Context):String = {
      if (!unSupport) {
        ctx.df.append(this.getString(ctx))
      }
      EmptyString
    }
    def getString(ctx:Context, expression: Expression = expr):String  = {
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
            ctx.addException(new Throwable(message))
            message
          }
          case column: Column => {
            val colName = column.getColumnName
            if (column.getTable != null) {
              val tableName = ctx.getTable(column.getTable)
              s"$tableName(" + "\"" + colName + "\"" + ")"
            } else {
              "\"" + colName + "\""
            }
          } // City or t1.name
          case func: Function => {
            if (func.getParameters != null) {
              val params = func.getParameters.getExpressions.toList
                .map(_.getString(ctx)).mkString(",")
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
                ctx.addException(new Throwable(message))
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
                val leftString = getColumnName(binaryExpr.getLeftExpression, ctx)
                val rightString = binaryExpr.getRightExpression.getString(ctx)
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

            val left = getColumnName(between.getLeftExpression,ctx)
            val start = between.getBetweenExpressionStart.getString(ctx)
            val end = between.getBetweenExpressionEnd.getString(ctx)
            s"$left >= $start and $left =< $end"
          }
          case _ => {
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
    def genCode(ctx:Context):String = {
      if (!unSupport) {
        val selects = body.getSelects.toList
        val operations = body.getOperations.toList
        val brackets = body.getBrackets

        for (i <- selects.indices) {
          if (i != 0) {
            val op = operations.get(i - 1).toString.toLowerCase
            if (op.equals("minus")) throw new UnsupportedOperationException("Unsupport Operation")
            ctx.df.append(" ").append(op).append(" ")
          }
          if (brackets == null || brackets.get(i)) {
            ctx.df.append("(")
            selects.get(i).genCode(ctx)
            ctx.df.append(")")
          } else {
            selects.get(i).genCode(ctx)
          }
        }

        if (body.getOrderByElements != null) genCodeOrderBy(body.getOrderByElements.toList,ctx, EmptyString)

        if (body.getLimit != null) genCodeLimit(body.getLimit, ctx)

        if (body.getOffset != null) ctx.df.append(body.getOffset.toString)

        if (body.getFetch != null) ctx.df.append(body.getFetch.toString)
      }
      EmptyString
    }
  }

  private def genCodeFrom(from:FromItem ,ctx:Context):mutable.StringBuilder  = {
    if (!unSupport){
      from match {
        case subjoin: SubJoin => {
          val leftTable = subjoin.getLeft.asInstanceOf[Table]
          ctx.addTable(leftTable)
          ctx.df.append(getTableName(leftTable))
          val joins = subjoin.getJoinList.toList
          joins.foreach(join => {
            ctx.addTable(join.getRightItem.asInstanceOf[Table])
            ctx.addJoin(join)
            join.genCode(ctx)
          })
          ctx.df
        }
        case table: Table => {
          ctx.addTable(table)
          val tableName = getTableName(table)
          ctx.df.append(tableName)
        }
        case parFrom: ParenthesisFromItem => {}
        case subselect: SubSelect => {
          subselect.getSelectBody.genCode(ctx)
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
    ctx.df
  }
  private def genCodeJoins(joins: List[Join] ,ctx:Context) = {
    if (!unSupport) {
      joins.foreach(join => {
        ctx.addTable(join.getRightItem.asInstanceOf[Table])
        ctx.addJoin(join)
        join.genCode(ctx)
      })
    }
    ctx.df
  }
  private def genCodeWhere(where:Expression,ctx:Context)  = {
    if (!unSupport) {
      val whereString = where.getString(ctx)
      ctx.df.append(s".filter($whereString)")
    }
    ctx.df
  }
  private def genCodeGroupBy(groupByElement: GroupByElement,ctx:Context)  = {
    var groupExpressionsString = EmptyString
    if (!unSupport) {
      groupExpressionsString = groupByElement
        .getGroupByExpressions
        .map( expression => {
          getColumnName(expression,ctx)
        })
        .mkString(",")
      ctx.df.append(s".groupBy($groupExpressionsString)")
    }
    groupExpressionsString
  }
  private def genCodeSelect(selectItems: List[SelectItem],ctx:Context,groupBy:String):String  = {
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
                val sExprString = func.getString(ctx)
                if(sExp.getAlias != null) {
                  sExprString + " as \"" + sExp.getAlias.getName +"\""
                } else {
                  sExprString
                }
              }
              case column:Column => {
                selectColumn = true
                if(sExp.getAlias != null) {
                  getColumnName(column,ctx) + ".as(\"" + sExp.getAlias.getName +"\")"
                } else {
                  getColumnName(column, ctx)
                }
              }
              case _ => {
                getColumnName(sExp.getExpression, ctx)
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
            ctx.logger.error("select item is wrong" + select)
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
        ctx.df.append("Current Version does not support to sellect column and agg")
        ctx.addException(new Throwable(ctx.df.toString()))
        return aggCols
      } else if ((!groupBy.isEmpty) && selectColumn){
        this.unSupport = true
        ctx.df.append("Current Version does not support groupBy operation without agg funcs in select")
        ctx.addException(new Throwable(ctx.df.toString()))
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
        ctx.df.append(s".agg($selectString)")
        aggCols = selectString
      } else
        ctx.df.append(s".select($selectString)")
    }
    return aggCols
  }
  private def genCodeHaving(havingExpr: Expression,ctx:Context, groupBy:String) = {
    if(groupBy.isEmpty){
      unSupport = true
      ctx.df.append("Need groupBy operation if you want to use having statement")
      ctx.addException(new Throwable(ctx.df.toString()))
      ctx.df
    } else {
      val havingString = havingExpr.getString(ctx)
      ctx.df.append(s".filter($havingString)")
    }
  }
  private def genCodeOrderBy(orderByElement: List[OrderByElement] ,ctx:Context, aggCols: String):mutable.StringBuilder  = {
    if (!unSupport) {
      var isFuncOrBinary:Boolean = false
      if (aggCols.isEmpty) {
        val eleString = orderByElement.map(ele => {
          if(ele.getExpression.isFuncOrBinary()) {
            isFuncOrBinary = true
          }
          val expStringList = ele.getExpression.getString(ctx).split("[()]") // column name will be in the last pos
          val order = if (!ele.isAsc) "desc"
          else if (ele.isAscDescPresent) "asc"
          else EmptyString

          if(order != EmptyString) { // User specify order way (asc or desc) explicitly
            order + "(" + expStringList.last + ")"
          } else {
            getColumnName(ele.getExpression, ctx)
          }
        }).mkString(",")
        ctx.df.append(s".orderBy($eleString)")
        if(isFuncOrBinary){
          this.unSupport = true
          ctx.df.append("Current Version does not support to order by with a func or binary operation")
          ctx.addException(new Throwable(ctx.df.toString()))
          return ctx.df
        }
      } else {
        this.unSupport = true
        ctx.df.append("Current Version does not support to order by from an agg selection without group by")
        ctx.addException(new Throwable(ctx.df.toString()))
        return ctx.df
      }
    }
    ctx.df
  }
  private def genCodeDistinct(distinct: Distinct ,ctx:Context)  = {
    if (!unSupport) {
      ctx.df.append(s".distinct")
    }
    ctx.df
  }
  private def genCodeLimit(limit: Limit ,ctx:Context)  = {
    if (!unSupport) {
      val nums = limit.getRowCount.getString(ctx)
      ctx.df.append(s".limit($nums)")
    }
    ctx.df
  }

  /*********************************************************************************************************/
  /****************************************   Helper Functions *********************************************/
  /*********************************************************************************************************/
  private def getTableName(table: Table) = table.getName

  private def resetEnvironment(): Unit ={
//    this.joinList.clear()
//    this.selectList.clear()
//    this.exceptionList.clear()
  }
  private def getColumnName(expression: Expression, ctx:Context):String = {
    val name = expression.getString(ctx)
    if(expression.isInstanceOf[Column])
      if(name.split("[()]").length > 1)
        name
      else
        "col(" + name + ")"
    else
      name
  }
//  def getDebugInfo(): Unit = {
//    ctx.logger.debug("[Table<Alias, Name>] " + tableList.mkString(","))
//    ctx.logger.debug("[Join] " + joinList.mkString(","))
//    ctx.logger.debug("[Select] " + selectList.mkString(","))
//    exceptionList.foreach(throwable => logger.debug(throwable.getCause))
//  }
}
