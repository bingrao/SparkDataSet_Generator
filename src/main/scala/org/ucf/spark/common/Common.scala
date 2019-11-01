package org.ucf.spark
package common

trait Common extends Logger{
  /**
    *
    * @param expectedDf  : Expected DataFrame Code
    * @param convertedDf : Converted DataFrame Code
    * @return
    */
  def compareDataFrame(expectedDf: String, convertedDf: String): Boolean = {
    expectedDf.toUpperCase.replaceAll("\\s", EmptyString) ==
      convertedDf.toUpperCase.replaceAll("\\s", EmptyString)
  }
}

