package org.ucf.spark
package dataframe

/**
  * @author
  */
import org.junit.Test
import org.junit.Assert._
import org.ucf.spark.codegen.DataFrame

class Select extends DataFrame{
  @Test def testSelectOneColumn(): Unit = {
    val sql = "SELECT City FROM Customers"
    val expected = "Customers.select(col(\"City\"))"
    assert(compareDataFrame(expected, codeGen(sql)))
  }
  @Test def testSelectWithAgg()= {
    val sql = "SELECT min(City) FROM Customers"
    val expected = "Customers.agg(min(\"City\"))"
    assert(compareDataFrame(expected, codeGen(sql)))
  }

  @Test def testSelectColumnAndAgg()= {
    val sql = "SELECT Name, min(City) FROM Customers"
    val expected = "Customers.agg(min(\"City\"))"
//    assert(compareDataFrame(expected, codeGen(sql)))
    logger.info(codeGen(sql))
  }

  @Test def testSelectColumnGroupBy()= {
    val sql = "SELECT Name, City FROM Customers group by Age"
    val expected = "Customers.agg(min(\"City\"))"
    //    assert(compareDataFrame(expected, codeGen(sql)))
    logger.info(codeGen(sql))
  }

  @Test def testSelectAggColumnGroupBy()= {
    val sql = "SELECT max(Age) FROM Customers group by City"
    val expected = "Customers.agg(min(\"City\"))"
    //    assert(compareDataFrame(expected, codeGen(sql)))
    logger.info(codeGen(sql))
  }

}
