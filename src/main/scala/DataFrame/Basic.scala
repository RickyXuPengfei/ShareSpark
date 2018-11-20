package DataFrame

import org.apache.spark.sql.SparkSession

object Basic {

  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("Basic")
        .master("local[2]")
        .getOrCreate()

    import spark.implicits._

    // create a sequence of case class object
    // define an RDD using custs, then convert an RDD into DataFrame
    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )

    // toDF
    val custDF = spark.sparkContext.parallelize(custs, 4).toDF()

    // toString
    println(s"toString():")
    println(custDF.toString())

    // printSchema
    println("use printSchema():")
    custDF.printSchema()

    // show()
    println("show():")
    custDF.show()

    // select
    println("select one column")
    custDF.select($"id").show()

    // select multiple columns
    println("select multiple columns")
    custDF.select($"sales", $"state", $"id").show()

    // filter
    println("filter to choose rows")
    custDF.filter($"state".equalTo("CA")).show()
  }
}
