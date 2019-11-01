package org.ucf.spark
package codegen

import java.io.{File, FileFilter}
import net.sf.jsqlparser.parser.CCJSqlParserUtil

import scala.io.Source
import scala.util.{Failure, Success, Try}

class Generator(context: Context = new Context()) extends common.EnrichedTrees {
  def genCodeFromSQLString(sql:String, ctx: Context = context) = {
    Try {
      CCJSqlParserUtil.parse(sql).genCode(ctx)
    } match {
      case Success(_) =>
      case Failure(ex) => throw new Exception(s"Exception while parsing following sql \n $sql " +
        s" \n Cause of exception is \n ${ex.getCause}")
    }
  }
  def getContext = this.context


  /**
  ├── database     ------>  root input path
    │   ├── academic   --> db folder 1, use folder name as db name
    │   │   ├── academic.sqlite  -- all data of DB
    │   │   └── schema.sql  --> all schema of tables in DB
    │   ├── activity_1
    │   │   ├── activity_1.sqlite
    │   │   └── schema.sql
    │   ├── aircraft
    │   │   ├── aircraft.sqlite
    │   │   └── schema.sql
    │   ├── allergy_1
    │   │   ├── allergy_1.sqlite
    │   │   └── schema.sql
    */
  def buildDatabases(path:String, ctx: Context = context) = {
    val pathDir = new File(path)
    if(pathDir.isDirectory){
      val subFolders = pathDir.listFiles( new FileFilter {
        override def accept(pathname: File): Boolean = pathname.isDirectory
      })
      subFolders.foreach( folder => buildSingleDatabase(folder))
    } else {
      logger.error(s"The input path is not a folder contains db files: $path")
    }
  }

  def buildSingleDatabaseFromPath(path:String, ctx: Context = context) =
    buildSingleDatabase(new File(path))

  def buildSingleDatabase(folder:File,ctx: Context = context) = {

    ctx.setCurretnDB(folder.getName)
    // get all sql file
    val sqlFiles = folder.listFiles( new FileFilter {
      override def accept(pathname: File): Boolean = pathname.getName.endsWith(".sql")
    })
    if(sqlFiles.length != 0)
      sqlFiles.foreach(sqlFile => {
        logger.debug(s"DB name: ${ctx.getCurrentDB.dbName}, SQL Path: ${sqlFile.getAbsolutePath}")
        createTable(sqlFile, ctx)
      }
      )
  }

  def createTable(sqlFile:File, ctx: Context = context) = {
    for (line <- Source.fromFile(sqlFile).getLines().mkString.split(";")){
      if(ctx.isSQLValidate(line)) {
        logger.debug(s"DB Name: ${ctx.getCurrentDB.getDatabaseName}, SQL Statement: $line")
        this.genCodeFromSQLString(line, ctx)
      }
    }
  }

  def preGenerator(sql:String, ctx: Context = context) = {
    logger.debug("******************************************\n\n")
    logger.debug("INPUT SQL: " + sql)
    // check current SQL if it is a valid sql statement
    if(!ctx.isSQLValidate(sql)){
      logger.error(s"The input is not a valid SQL statement: $sql")
      ctx.disableSupport
    }
  }
  def postGenerator(sql:String, ctx: Context = context) = {
    // Print out debug info
    if(logger.isDebugEnabled)
      ctx.getDebugInfo()

    //After generator, the context need to be reset for next run
    ctx.reset
  }
  def run(sql:String, ctx: Context = context):String = {
    preGenerator(sql)
    genCodeFromSQLString(sql,ctx)
    if(!ctx.isSupport) {
      ctx.enableSupport
      //      logger.info(s"${unSupportNotice} INPUT SQL: ${sql} ${unSupportNotice}")
      //      logger.info(s"${unSupportNotice} OUTPUT DataFrame: ${dataframe.toString()} ${unSupportNotice}\n")
      ctx.append(unSupportNotice) // doest not remove this statement, will used in spider filter
    }
    val reg = ctx.getSparkDataFrame
    logger.debug("OUTPUT DataFrame: " + reg)
    postGenerator(sql)
    reg
  }
}