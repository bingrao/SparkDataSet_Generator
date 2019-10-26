package org.ucf.spark
/**
  * @author
  */
import org.junit.Test
import org.junit.Assert._

class ScalaTestAPP extends common.Common {
  val df = new dataframe.DataFrame
  @Test def testAdd() {
    logger.info("Hello World From Scala")
    assertTrue(true)
  }
  @Test def testParseSQL(): Unit = {
    val s1 = "SELECT City FROM Customers UNION SELECT City FROM Suppliers ORDER BY City LIMIT 10;"
    val s2 = "SELECT * FROM Suppliers ORDER BY City, County LIMIT 10;"
    val s3 = "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate FROM Orders INNER JOIN Customers ON Orders.CustomerID=Customers.CustomerID;"
    val s4 = "SELECT City FROM Customers"
    val s5 = "SELECT T1.creation FROM department AS T1 JOIN management AS T2 ON T1.department_id  =  T2.department_id JOIN head AS T3 ON T2.head_id  =  T3.head_id WHERE T3.born_state  =  'Alabama'"
    val s6 = "SELECT Shippers.ShipperName, COUNT(Orders.OrderID) AS NumberOfOrders FROM Orders LEFT JOIN Shippers ON Orders.ShipperID = Shippers.ShipperID GROUP BY ShipperName;"
    logger.info(df.parse(s5))
  }
}