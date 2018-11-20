package DataFrame

import org.apache.spark.sql.SparkSession

object DatasetConversions {
  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  case class StateSales(state: String, sales: Double)

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("DatasetConversion")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    val custes = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )

    val custDF = spark.sparkContext.parallelize(custes).toDF()

    println("*** DataFrame schema")

    custDF.printSchema()

    println("*** DataFrame contents")

    custDF.show()

    println("*** Select and filter the DataFrame")

    val smallerDF =
      custDF.select("sales", "state").filter($"state".equalTo("CA"))

    smallerDF.show()

    // Convert DataFrame into a Dataset by specify the type of rows
    val custDS = smallerDF.as[StateSales]

    println("Dataset schema")
    custDS.printSchema()

    println("Dataset contents")
    custDS.show()

    custDS.cache()

    val verySmallDS = custDS.select($"sales".as[Double])

    println("*** Dataset after projecting one column")

    verySmallDS.show()

    val tupleDS = custDS.select($"state".as[String], $"sales".as[Double])

    println("*** Dataset after projecting two columns -- tuple version")

    tupleDS.show()

    // back to a Dataset of a case class
    val betterDS = tupleDS.as[StateSales]
    println("*** Dataset after projecting two columns -- case class version")

    betterDS.show()

    // Convert back to a DataFrame
    val backDF = tupleDS.toDF()
    println("*** This time as a DataFrame")

    backDF.show()

  }
}
