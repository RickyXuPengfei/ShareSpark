package SparkSQL

import org.apache.spark.sql.SparkSession

object CaseWhenThen {
  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("CaseWhenThen")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    // create a sequence of case class objects
    // (we defined the case class above)
    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    // make it an RDD and convert to a DataFrame
    val customerDF = spark.sparkContext.parallelize(custs, 4).toDF()

    println("*** See the DataFrame contents")
    customerDF.show()

    println("*** A DataFrame has a schema")
    customerDF.printSchema()

    // Register table
    customerDF.createOrReplaceTempView("customer")

    // CASE WHEN THEN  ELSE
    println("CASE WHEN Examples")
    val caseWhen = spark.sql(
      s"""
         | SELECT CASE
         |    WHEN id = 1 THEN "One"
         |    WHEN id = 2 THEN "Two"
         |    ELSE "Other"
         |    END AS IdRedux
         | FROM customer
       """.stripMargin)

    caseWhen.printSchema()
    caseWhen.show()

    // boolean expression
    println("With boolean combination")
    val caseWhen2 = spark.sql(
      s"""
         |  SELECT
         |    CASE WHEN id = 1 or id = 2 THEN "OneOrTwo"
         |    ELSE "NotOneOrTwo"
         |    END AS IdRedux
         |  FROM customer
       """.stripMargin)
    caseWhen2.printSchema()
    caseWhen2.show()

    // More than one column
    val caseWhen3 = spark.sql(
      s"""
         | SELECT
         |  CASE WHEN id = 1 or state = 'MA' THEN "OneOrMA"
         |  ELSE "NotOneOrMA"
         |  END AS IdRedux
         | FROM customer
       """.stripMargin)
    caseWhen3.printSchema()
    caseWhen3.show()

    // Conditions can be nested
    println("With nested conditions")
    val caseWhen4 = spark.sql(
      s"""
         | SELECT
         |  CASE WHEN id = 1 THEN "OneOrMA"
         |  ELSE
         |    CASE WHEN state = "MA" THEN "OneOrMA"
         |    ELSE "NotOneOrMA" END
         |  END AS idRedux
         | FROM customer
       """.stripMargin)

    caseWhen4.printSchema()
    caseWhen4.show()
  }
}
