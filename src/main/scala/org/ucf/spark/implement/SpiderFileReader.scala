package org.ucf.spark
package implement
import java.io.FileWriter
import scala.io.Source
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization.write
import org.json4s.{DefaultFormats, JObject, JString}


/**
  * https://www.hackingnote.com/en/scala/json4s
  */
object SpiderFileReader {
  def parser(path:String = "input/train_spider.json") = {
    implicit val formats = DefaultFormats
    val source = Source.fromFile(path)
    val jObject = parse(source.reader()).extract[List[JObject]]
      .filter(ele => {
        val query = (ele \ "query").extract[String].toUpperCase
        var reg:Boolean = true
        if (query.split(" ").contains("START")) reg = false
        if (query.split(" ").contains("T2.START")) reg = false
        if (query.split(" ").contains("MATCH")) reg = false
        if (query.split(" ").contains("CHARACTER")) reg = false
        if (query.split(" ").contains("NOT")) reg = false
        if (query.split(" ").contains("HAVING")) reg = false
        if (query.contains("ORDER BY COUNT(*)  >=  5")) reg = false
        reg
      })
      .map( ele => {
      val query = (ele \ "query").extract[String]
      ele merge JObject("SparkDataFrame" -> JString(codegen.DataFrame.codeGen(query)))
    })

    val fw = new FileWriter("output/train_spider_output.json")

    fw.write(write(jObject))
    fw.close()
  }
}