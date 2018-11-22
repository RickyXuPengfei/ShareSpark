package SparkSQL

import java.sql.Date

import org.apache.spark.sql.SparkSession

object ComplexTypes {
  case class Address(state:String)

  case class Transaction(id: Integer, date: Date, amount:Double)

  case class Cust(id: Integer, name:String, trans: Seq[Transaction], billing:Address, shipping:Address)

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("SQL-ComplexTypes")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    val custs = Seq(
      Cust(1, "Widget Co",
        Seq(Transaction(1, Date.valueOf("2012-01-01"), 100.0)),
        Address("AZ"), Address("CA")
      ),
      Cust(2, "Acme Widgets",
        Seq(Transaction(2, Date.valueOf("2014-06-15"), 200.0),
          Transaction(3, Date.valueOf("2014-07-01"), 50.0)),
        Address("CA"), null),
      Cust(3, "Widgetry",
        Seq(Transaction(4, Date.valueOf("2014-01-01"), 150.0)),
        Address("CA"), Address("CA"))
    )

    val customDF = spark.sparkContext.parallelize(custs,2).toDF()

    println("*** inferred schema takes nesting and arrays into account")
    customDF.printSchema()

    customDF.createOrReplaceTempView("customer")

    println("*** Query results reflect complex structure")
    val allCust = spark.sql("SELECT * FROM customer")
    allCust.show()

    //
    // notice how the following query refers to shipping.date and
    // trans[1].date but some records don't have these components: Spark SQL
    // handles these cases quite gracefully
    //

    println("*** Projecting from deep structure doesn't blow up when it's missing")
    val projectedCust =
      spark.sql(
        s"""
           | SELECT id, name, shipping.state, trans[1].date AS secondTrans
           | FROM customer
         """.stripMargin)
    projectedCust.printSchema()
    projectedCust.show()


    //
    // The 'trans' field is an array of Transaction structures, but you can pull just the
    // dates out of each one by referencing trans.date
    //

    println("*** Reach into each element of an array of structures by omitting the subscript")
    val arrayOfStruct = spark.sql(
      s"""
         | SELECT shipping.state, trans.date AS transDates FROM customer
       """.stripMargin)
    arrayOfStruct.printSchema()
    arrayOfStruct.show()

    println("*** Group by a nested field")
    val groupedByNeseted = spark.sql(
      s"""
         | SELECT shipping.state, count(*) as customers
         | FROM customer
         | GROUP BY shipping.state
       """.stripMargin)

    groupedByNeseted.printSchema()
    groupedByNeseted.show()

    println("*** Order by a nested field")
    val orderByNested = spark.sql(
      s"""
         | SELECT id, shipping.state
         | FROM customer
         | ORDER BY shipping.state
       """.stripMargin)

    orderByNested.printSchema()
    orderByNested.show()
  }

}
