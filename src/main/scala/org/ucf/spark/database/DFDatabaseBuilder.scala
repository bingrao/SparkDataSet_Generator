package org.ucf.spark
package database

import scala.collection.mutable
import java.io.{File, FileFilter}

import scala.io.Source

class DFDatabaseBuilder {
  private val databases = mutable.HashMap[String, DFDatabase]()

  def getDB(db:String) = databases.getOrElseUpdate(db, new DFDatabase(db))

  def createTable(db:DFDatabase, sqlFile:File) = {
    for (line <- Source.fromFile(sqlFile).getLines()){
    }
  }
  def buildDatabases(path:String) = {
    val pathDir = new File(path)
    if(pathDir.isDirectory){
      val subFolders = pathDir.listFiles( new FileFilter {
        override def accept(pathname: File): Boolean = pathname.isDirectory
      })
      subFolders.foreach( folder => {
        val db = getDB(folder.getName)

        val sqlFiles = folder.listFiles( new FileFilter {
          override def accept(pathname: File): Boolean = pathname.getName.endsWith(".sql")
        })
        if(sqlFiles.length != 0)
          sqlFiles.foreach( sqlFile => createTable(db, sqlFile))
      })
    } else {

    }
  }




}
