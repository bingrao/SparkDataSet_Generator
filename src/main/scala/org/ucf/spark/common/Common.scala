package org.ucf.spark
package common

trait Common {
  val logger = new utils.Log(this.getClass.getName)
}

