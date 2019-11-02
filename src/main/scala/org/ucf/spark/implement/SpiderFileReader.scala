package org.ucf.spark
package implement

import java.io.{File, FileFilter, FileWriter}

import org.json4s.{DefaultFormats, JObject, JString}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization.writePretty

import scala.io.Source
import codegen._
import net.sf.jsqlparser.statement.create.table.{ColDataType, ColumnDefinition}
/**
  * https://www.hackingnote.com/en/scala/json4s
  */
object SpiderFileReader extends common.Logger {
  val files = Map("data/spider/dev.json" -> "data/output/spider/dev.df.json",
    "data/spider/train_spider.json" ->"data/output/spider/train_spider.df.json",
    "data/spider/train_others.json" ->"data/output/spider/train_others.df.json")
  val generator = new Generator()
  def genSparkDataFrame() = {
    files.foreach{ case (input, output) =>
      val inFile = new File(input)
      if(inFile.isFile){
        logger.info(s"Start Converst SQL to Spark DataFrame from ${input} to ${output}")
        parseJSON(input, output,"query")
        logger.info("Conversation is over!!\n\n")
      } else{
        logger.info(s"The input does not exist: ${input} to ${output}")
      }
    }
  }
  def parseJSON(inPath:String, outPath:String, queryString: String = "query") = {
    implicit val formats = DefaultFormats
    val source = Source.fromFile(inPath)
    val target = new FileWriter(outPath)
    try {
      val jsonObject = parse(source.reader()).extract[List[JObject]]
      val tgtObject = jsonObject
        .filter(ele => generator.getContext.isSQLValidate((ele \ queryString).extract[String]))
        .map(ele => {
          val query = (ele \ "query").extract[String]
          ele merge JObject(DFObject -> JString(generator.run(query)))
        }).filter(ele => !(ele \ DFObject).extract[String].contains(unSupportNotice))
      logger.info(s"The valid object from ${jsonObject.size} to ${tgtObject.size} in ${inPath}")
      target.write(writePretty(tgtObject) + "\n")

    } catch  {
      case e: Exception => logger.info("exception caught: " + e)
    } finally {
      target.close()
      source.close()
    }
  }

  def builSpiderDatabaseFromJSON(inPath:String, generator: Generator) = {
    implicit val formats = DefaultFormats
    val source = Source.fromFile(inPath)
    try {
      val jObject = parse(source.reader()).extract[List[JObject]]
        .foreach(ele => {
          val db = generator.getContext.getOrElseUpdateDB((ele \ "db_id").extract[String].toLowerCase)
          val tables = (ele \ "table_names_original").extract[List[String]]
          val colums = (ele \ "column_names_original").extract[List[List[String]]]
          val columTypes = (ele \ "column_types").extract[List[String]]
          for (colIdx <- colums.indices) {
            val column = colums(colIdx)
            val columType = new ColDataType
            columType.setDataType(columTypes(colIdx))
            val tableIdx = column.head.toInt
            val colName = column.last.toLowerCase
            if (tableIdx != -1) {
              val table = db.getOrElseUpdate(tables(tableIdx).toLowerCase)
              table.setIndex(tableIdx)
              val colDef = new ColumnDefinition()
              colDef.setColumnName(colName)
              colDef.setColDataType(columType)
              table.addColumnDefinition(colDef)
            }
          }
        })
    } catch  {
      case e: Exception => logger.info("exception caught: " + e)
    } finally {
      source.close()
    }
  }

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
  def buildSpiderDatabases(path:String = "data/spider/database", generator: Generator) = {
    val pathDir = new File(path)
    if(pathDir.isDirectory){
      val subFolders = pathDir.listFiles( new FileFilter {
        override def accept(pathname: File): Boolean = pathname.isDirectory
      })
      subFolders.foreach( folder => buildSingleDatabase(folder, generator))
    } else {
      logger.error(s"The input path is not a folder contains db files: $path")
    }
  }
  def buildSingleDatabaseFromPath(path:String, generator: Generator) =
    buildSingleDatabase(new File(path), generator)

  def buildSingleDatabase(folder:File,generator: Generator) = {

    generator.getContext.setCurretnDB(folder.getName.toLowerCase)
    // get all sql file
    val sqlFiles = folder.listFiles( new FileFilter {
      override def accept(pathname: File): Boolean = pathname.getName.endsWith(".sql")
    })
    if(sqlFiles.length != 0)
      sqlFiles.foreach(sqlFile => createTable(sqlFile, generator))
  }

  def createTable(sqlFile:File, generator: Generator) = {
    val createStatement = Source.fromFile(sqlFile)
      .getLines().mkString.split(";").filter(_.toUpperCase.contains("CREATE TABLE"))
    for (line <- createStatement if(generator.getContext.isSQLValidate(line)))
      generator.run(line)
  }

  def builderDatabaseFromSchema(path:String = "data/test") = {
//    buildSpiderDatabases("data/spider/database", generator) // a folder contains all db's sql file
    builSpiderDatabaseFromJSON("data/spider/tables.json", generator)
    val dbs = generator.getContext.allDBToString
    logger.info(dbs,false)
  }
}