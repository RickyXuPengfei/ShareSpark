package Streaming.structed

import Streaming.util.CSVFileStreamGenerator
import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Basic {
  def main(args: Array[String]): Unit = {
    val fm = new CSVFileStreamGenerator(10, 5, 500)

    println("*** Starting to stream")

    val spark = SparkSession
      .builder
      .appName("StructuredStreaming_Basic")
      .config("spark.master", "local[4]")
      .getOrCreate()

    val recordSchema = StructType(
      Seq(StructField("key", StringType),
        StructField("value", IntegerType))
    )

    val csvDF = spark.readStream.option("sep",",").schema(recordSchema).csv("file://" + fm.dest.getAbsolutePath)

    csvDF.printSchema()

    val query = csvDF.writeStream.outputMode("append").format("console").start()

    println("*** done setting up streaming")

    Thread.sleep(5000)

    println("*** now generating data")
    fm.makeFiles()

    Thread.sleep(5000)
    println("*** Stopping stream")
    query.stop()

    query.awaitTermination()
    println("*** Streaming terminated")

    println("*** done")


  }
}
