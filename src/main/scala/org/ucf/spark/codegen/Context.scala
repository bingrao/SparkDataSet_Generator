package org.ucf.spark
package codegen

import org.ucf.spark.utils.PropertiesLoader
class Context(configPath:String = "src/main/resources/application.conf") extends common.Common {
  val parameters = new PropertiesLoader(configPath)
  val df = new StringBuilder()
}
