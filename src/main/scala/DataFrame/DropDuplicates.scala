package DataFrame

import org.apache.spark.sql.SparkSession

object DropDuplicates {

  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("DropDuplicates")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    // create an RDD of tuples with some data
    val custs = Seq(
      (1, "Widget Co", 120000.00, 0.00, "AZ"),
      (2, "Acme Widgets", 410500.00, 500.00, "CA"),
      (3, "Widgetry", 410500.00, 200.00, "CA"),
      (4, "Widgets R Us", 410500.00, 0.0, "CA"),
      (3, "Widgetry", 410500.00, 200.00, "CA"),
      (5, "Ye Olde Widgete", 500.00, 0.0, "MA"),
      (6, "Widget Co", 12000.00, 10.00, "AZ")
    ).map(row => Cust(row._1, row._2, row._3, row._4, row._5))

    val custDF = spark.sparkContext.parallelize(custs, 4).toDF()

    custDF.printSchema()

    custDF.show()

    // drop fully rows
    val withoutDuplicates = custDF.dropDuplicates()

    println("drop fully rows")
    withoutDuplicates.show()

    // drop row with specific columns
    val withoutPartials = custDF.dropDuplicates(Seq("name","state"))
    println("Without partial duplicates ")
    withoutPartials.show()

  }

}
