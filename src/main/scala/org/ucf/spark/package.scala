package org.ucf
package object spark extends Enumeration{
  final val unSupportNotice = "[***UNSUPPORT***]"
  final val EmptyString:String = ""  // recursive function return value for gencode func in implicit class

  // input file format
  val JSON = Value("Json")
  val JSONL = Value("Jsonl")
  val NORMAL = Value("Normal")

}
