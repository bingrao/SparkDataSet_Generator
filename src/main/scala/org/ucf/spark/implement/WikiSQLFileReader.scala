package org.ucf.spark
package implement

import java.io.{File, FileWriter}

import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization.{write, writePretty}
import org.json4s.{DefaultFormats, JObject, JString}
import org.ucf.spark.codegen.{Context, Generator}
import scala.io.Source


/**
  * https://www.hackingnote.com/en/scala/json4s
  */

object WikiSQLFileReader extends common.Logger {
  val files = Map("data/WikiSQL/data/dev_query.jsonl" -> "data/output/WikiSQL/dev.df.jsonl",
    "data/WikiSQL/data/test_query.jsonl" ->"data/output/WikiSQL/test.df.jsonl",
    "data/WikiSQL/data/train_query.jsonl" ->"data/output/WikiSQL/train.df.jsonl")
  val generator = new Generator()
  def genSparkDataFrame() = {
    files.foreach{ case (input, output) =>
      val inFile = new File(input)
      if(inFile.isFile){
        logger.info(s"Start Converst SQL to Spark DataFrame from ${input} to ${output}")
        parseJSONL(input, output)
        logger.info("Conversation is over!!\n\n")
      } else{
        logger.info(s"The input does not exist: ${input} to ${output}")
      }
    }
  }
  def parseJSONL(inPath:String, outPath:String, queryString: String = "queryString") = {
    implicit val formats = DefaultFormats
    val source = Source.fromFile(inPath)
    val target = new FileWriter(outPath)
    try {
      val jsonObject = source.getLines().toList.map( list => {
        parse(list).extract[JObject]
      })
      val tgtObject = jsonObject
        .filter(ele => generator.getContext.isSQLValidate((ele \ queryString).extract[String]))
        .map( ele => {
          val query = (ele \ queryString).extract[String]
          ele merge JObject(DFObject -> JString(generator.run(query)))
        }).filter( ele => !(ele \ DFObject).extract[String].contains(unSupportNotice))
      logger.info(s"The valid object from ${jsonObject.size} to ${tgtObject.size} in ${inPath}")
      target.write(tgtObject.map(write(_)).mkString("\n"))
    } catch {
      case e: Exception => logger.info("exception caught: " + e)
    } finally {
      target.close()
      source.close()
    }
  }
}