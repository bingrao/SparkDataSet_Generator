package org.ucf.spark
package common

trait Logger {
  val logger = new utils.Log(this.getClass.getName)
}
