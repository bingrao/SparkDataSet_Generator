package org.ucf.spark
package implement

import java.io.{File, FileWriter}

import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization.{write, writePretty}
import org.json4s.{DefaultFormats, JObject, JString}
import org.ucf.spark.codegen.DataFrame
import scala.io.Source

/**
  * https://www.hackingnote.com/en/scala/json4s
  */

object WikiSQLFileReader extends DataFrame{
  def genSparkDataFrame() = {
    val files = Map("data/WikiSQL/data/dev_query.jsonl" -> "data/output/WikiSQL/dev_query.df.jsonl",
      "data/WikiSQL/data/test_query.jsonl" ->"data/output/WikiSQL/test_query.df.jsonl",
      "data/WikiSQL/data/train_query.jsonl" ->"data/output/WikiSQL/train_query.df.jsonl")
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
  def parser(inPath:String, outPath:String, queryString: String = "queryString") = {
    implicit val formats = DefaultFormats
    val source = Source.fromFile(inPath)
    val fw = new FileWriter(outPath)
    source.getLines().toList.map( list => {
      parse(list).extract[JObject]
    }).filter(ele => isSQLValidate((ele \ queryString).extract[String]))
      .map( ele => {
        val query = (ele \ queryString).extract[String]
        ele merge JObject("SparkDataFrame" -> JString(codeGen(query)))
      }).filter( ele => !(ele \ "SparkDataFrame").extract[String].contains(unSupportNotice))
      .foreach( jobject => {
        fw.write(write(jobject) + "\n")
      })
    fw.close()
    source.close()
  }
}