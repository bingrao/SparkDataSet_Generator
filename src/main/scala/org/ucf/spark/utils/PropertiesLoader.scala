package org.ucf.spark
package utils

import java.io.File
import com.typesafe.config.{Config, ConfigFactory}
class PropertiesLoader(configPath:String = "src/main/resources/application.conf") {

  val parsedConfig = ConfigFactory.parseFile(new File(configPath))

  private val conf = ConfigFactory.load(parsedConfig)

  val getInputPath = conf.getString("InputPath")
  val getInputFileSep = conf.getString("InputSep")
  val getInputFileFormat = conf.getString("InputFormat").toUpperCase  match {
    case "JSON" => JSON
    case "JSONL" => JSONL
    case _ => NORMAL
  }


  val getOutputPath = conf.getString("OutputPath")
  val getOutputSep = conf.getString("OutputSep")
  val getOutputFormat = conf.getString("OutputFormat").toUpperCase  match {
    case "JSON" => JSON
    case "JSONL" => JSONL
    case _ => NORMAL
  }
  val getPrettyOutput = conf.getString("OutputPretty")

  val getLogLevel = conf.getString("LogLevel")

}