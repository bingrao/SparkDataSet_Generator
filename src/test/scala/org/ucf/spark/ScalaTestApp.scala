package org.ucf.spark
/**
  * @author
  */
import org.junit.Test
import org.junit.Assert._

class ScalaTestAPP extends DFTestBase {

  @Test def testAdd() {
    logger.info("Hello World From Scala")
    assertTrue(true)
  }

  val s3 = "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate FROM Orders INNER JOIN Customers ON Orders.CustomerID=Customers.CustomerID;"
  val s4 = "SELECT City FROM Customers"
  val s5 = "SELECT T1.creation FROM department AS T1 JOIN management AS T2 ON T1.department_id  =  T2.department_id JOIN head AS T3 ON T2.head_id  =  T3.head_id WHERE T3.born_state  =  'Alabama'"
  val s6 = "SELECT Shippers.ShipperName, COUNT(Orders.OrderID) AS NumberOfOrders FROM Orders LEFT JOIN Shippers ON Orders.ShipperID = Shippers.ShipperID GROUP BY ShipperName;"

  @Test def testParseSQL_s1(): Unit = {
    val s1 = "SELECT City FROM Customers UNION SELECT City FROM Suppliers ORDER BY City LIMIT 10;"
    logger.info(s"INPUT: ${s5}")
    logger.info("OUTPUT: " + codeGen(s5))
  }

  @Test def testParseSQL_s2(): Unit = {
    val s2 = "SELECT * FROM Suppliers ORDER BY City, County LIMIT 10;"
    logger.info(s"INPUT: ${s2}")
    logger.info("OUTPUT: " + codeGen(s2))
  }
  @Test def testParseSQL_s7(): Unit = { // cannot parse since >= in order by
    val s7 = "SELECT T2.name FROM Certificate AS T1 JOIN Aircraft AS T2 ON T2.aid  =  T1.aid WHERE T2.distance  >  5000 GROUP BY T1.aid ORDER BY count(*)  >=  5"  // problem at "ORDER BY count(*)  >=  5"
    val s8 = "SELECT appointmentid FROM appointment ORDER BY START DESC LIMIT 1" //start
    val s9 = "SELECT Character, Duration FROM actor" // "Character"
    val s10 = "SELECT T1.name FROM patient AS T1 JOIN appointment AS T2 ON T1.ssn = T2.patient ORDER BY T2.start DESC LIMIT 1" // start
    val s11 = "SELECT T2.Location ,  T1.Aircraft FROM aircraft AS T1 JOIN MATCH AS T2 ON T1.Aircraft_ID  =  T2.Winning_Aircraft" //match
    val s12 = "SELECT T1.Aircraft FROM aircraft AS T1 JOIN MATCH AS T2 ON T1.Aircraft_ID  =  T2.Winning_Aircraft GROUP BY T2.Winning_Aircraft ORDER BY COUNT(*) DESC LIMIT 1" //match
    val s13 = "SELECT venue FROM MATCH ORDER BY date DESC"  // match
    val s14 = "SELECT Aircraft FROM aircraft WHERE Aircraft_ID NOT IN (SELECT Winning_Aircraft FROM MATCH)"  //match
    val s15 = "SELECT count(DISTINCT temporary_acting) FROM management"
    val s16 = "INPUT SQL: SELECT head_id ,  name FROM head WHERE name LIKE '%Ha%'"
//    logger.info(s"INPUT: ${s8}")
    logger.info("OUTPUT: " + codeGen(s15))
  }

  @Test def testParseSQL_s3(): Unit = {
    val s3 = "SELECT max(budget_in_billions) as Bing FROM department"
    logger.info(s"INPUT: ${s3}")
    logger.info("OUTPUT: " + codeGen(s3))
  }

  @Test def testParseSQL_product(): Unit = {
    val s3 = "Select count(*), max(price) from product group by product.brand"
    val s4 = "SELECT category FROM book_club WHERE YEAR  >  1989 GROUP BY category"
    val s5 = "SELECT count(*) FROM department AS T1 JOIN management AS T2 ON T1.department_id  =  T2.department_id JOIN head AS T3 ON T2.head_id  =  T3.head_id WHERE T3.born_state  =  'Alabama'"
    val s6 = "SELECT T2.first_name , T2.last_name FROM employees AS T1 JOIN employees AS T2 ON T1.id = T2.reports_to WHERE T1.first_name LIKE 10;"
    val s7 ="Select min(asin), max(price) as maxPrice from product group by brand having maxPrice > 100"
    val s8 = "SELECT river_name FROM river WHERE traverse IN ( SELECT state_name FROM city WHERE population  =  ( SELECT MAX ( population ) FROM city ) );"
    val test = s8
    logger.info(s"INPUT: ${test}")
    logger.info("OUTPUT: " + codeGen(test))
  }
}