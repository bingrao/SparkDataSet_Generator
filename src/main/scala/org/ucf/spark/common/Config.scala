package org.ucf.spark.common

/**
  * Configurations
  */
object Config {

  /**
    * End of the SQL indicator
    * In SAS ); will be EndSqlInd
    */

  final val EndSqlInd = ";"

  /**
    * Columns which contain following identifiers are taken as Date Columns
    */
  val DateIdentifiers = List("date", "DATE", "_dte", "_DTE")

  /**
    * Sas Sql to spark SQL related config
    */

  final val InputFilePath = "input/inputSasSql.sas"
  final val SqlOutputFilePath = "output/outputForSasSql.scala"

  final val SqlFileName = "Sql"

  final val SqlFileInitialization: String =
    s"""
       |import java.util._
       |import org.apache.spark.sql.SparkSession
       |import org.apache.spark.sql.functions._
       |
       |object $SqlFileName{
       |def main(args: Array[String]): Unit = {
       |
                                    |
                                    |""".stripMargin

  /**
    * Spark Sql to Spark DataFrame related config
    */

  final val DfInputFilePath = SqlOutputFilePath
  final val DfOutputFilePath = "output/outputForSasSqlDf.scala"

  final val DfFileName = "PropensityScoring"

  final val DfFileInitialization: String =
    s"""
       |import java.util._
       |import org.apache.spark.sql.SparkSession
       |import org.apache.spark.sql.functions._
       |
                          |object $DfFileName{
       |def main(args: Array[String]): Unit = {
       |
                          |
                          |""".stripMargin


  final val GlobalConstants: List[String] = List("null")
}
