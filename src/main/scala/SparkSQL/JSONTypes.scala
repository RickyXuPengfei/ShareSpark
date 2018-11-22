package SparkSQL

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DateType, StructField, StructType, TimestampType}


object JSONTypes {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("SQL-JSONTypes")
        .master("local[4]")
        .getOrCreate()

    val schema = StructType(
      Seq(
        StructField("date",DateType, true),
        StructField("ts",TimestampType, true)
      )
    )

    val rows = spark.sparkContext.parallelize(Seq(
      Row(new Date(3601000),
          new Timestamp(3601000))
    ), 4)
    val tdf = spark.createDataFrame(rows, schema)

    println("directly print")
    tdf.foreach(print(_))

    println("toJSON, then println")
    tdf.toJSON.foreach(println(_))


    val text = spark.sparkContext.parallelize(Seq(
      "{\"date\":\"1969-12-31\",\"ts\":\"1969-12-31 17:00:01.0\"}"
    ), 4)

    import spark.implicits._
    // call toDS on RDD
    val json1 = spark.read.json(text.toDS())
    json1.printSchema()

    json1.createOrReplaceTempView("json1")
    spark.sql("select * from json1").show()
  }
}
