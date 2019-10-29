package org.ucf.spark
package implement

import java.io.{File, FileWriter}

import org.json4s.{DefaultFormats, JObject, JString}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization.writePretty
import org.ucf.spark.codegen.DataFrame

import scala.io.Source

/**
  * https://www.hackingnote.com/en/scala/json4s
  */
object SpiderFileReader extends DataFrame{
  def genSparkDataFrame() = {
    val files = Map("data/spider/dev.json" -> "data/output/spider/dev.df.json",
      "data/spider/train_spider.json" ->"data/output/spider/train_spider.df.json",
      "data/spider/train_others.json" ->"data/output/spider/train_others.df.json")
    files.foreach{ case (input, output) =>
      val inFile = new File(input)
      if(inFile.isFile){
        logger.info(s"Start Converst SQL to Spark DataFrame from ${input} to ${output}")
        parser(input, output)
        logger.info("Conversation is over!!\n\n")
      } else{
        logger.info(s"The input does not exist: ${input} to ${output}")
      }
    }
  }
  def parser(inPath:String, outPath:String, queryString: String = "query") = {
    implicit val formats = DefaultFormats
    val source = Source.fromFile(inPath)
    val jObject = parse(source.reader()).extract[List[JObject]]
      .filter( ele => isSQLValidate((ele \ queryString).extract[String]))
      .map( ele => {
        val query = (ele \ "query").extract[String]
        ele merge JObject("SparkDataFrame" -> JString(codeGen(query)))
      }).filter( ele => !(ele \ "SparkDataFrame").extract[String].contains(unSupportNotice))

    val fw = new FileWriter(outPath)
    fw.write(writePretty(jObject) + "\n")
    fw.close()
    source.close()
  }
}