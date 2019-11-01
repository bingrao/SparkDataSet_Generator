package org.ucf

import org.apache.log4j.Logger

import scala.collection.mutable

package object spark extends Enumeration{
  final val unSupportNotice = "[***UNSUPPORT***]"
  final val EmptyString:String = ""  // recursive function return value for gencode func in implicit class

  // input file format
  val JSON = Value("Json")
  val JSONL = Value("Jsonl")
  val NORMAL = Value("Normal")

  final val DFObject = "SparkDataFrame"

  // add log functions to all Scala Objects
  implicit class AddLogger(any:AnyRef) {
    //https://alvinalexander.com/scala/scala-functions-repeat-character-n-times-padding-blanks
    def getIndent(nums:Int) = "\t" * nums
    def printPretty(sb:mutable.StringBuilder, numsIntent:Int) = sb.append("")
  }
}
