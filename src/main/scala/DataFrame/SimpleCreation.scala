package DataFrame

import org.apache.spark.sql.SparkSession

object SimpleCreation {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("SimpleCreation")
        .master("local[2]")
        .getOrCreate()

    import spark.implicits._


    val custs = Seq(
      (1, "Widget Co", 120000.00, 0.00, "AZ"),
      (2, "Acme Widgets", 410500.00, 500.00, "CA"),
      (3, "Widgetry", 410500.00, 200.00, "CA"),
      (4, "Widgets R Us", 410500.00, 0.0, "CA"),
      (5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )

    // convert RDD to DataFrame
    // note: spark.implicits

    // RDD
    val custRDDs = spark.sparkContext.parallelize(custs, 4)
    val custDF = custRDDs.toDF("id", "names", "sales", "discounts", "state")

    custDF.printSchema()
    custDF.show()
  }
}
