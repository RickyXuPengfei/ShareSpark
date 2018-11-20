package DataFrame

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}


object FromRowsAndSchema {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("FromRowsAndSchema")
        .master("local[2]")
        .getOrCreate()

    // create RDD using ROW
    val custs = Seq(
      Row(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Row(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Row(3, "Widgetry", 410500.00, 200.00, "CA"),
      Row(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Row(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    val custRDD = spark.sparkContext.parallelize(custs, 4)

    // state Schema
    // StructType(Seq(StructField(name, type, nullable)....))
    val custSchema = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("names", StringType, true),
        StructField("sales", DoubleType, true),
        StructField("discount", DoubleType, true),
        StructField("state", StringType, true)
      )
    )

    // createDataFrame(RDD[Row], schema)
    val custDF = spark.createDataFrame(custRDD, custSchema)

    custDF.printSchema()

    custDF.show()

  }
}
