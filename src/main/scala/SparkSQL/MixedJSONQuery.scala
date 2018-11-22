package SparkSQL

import java.io.File
import java.nio.file.Paths

import org.apache.spark.sql.SparkSession

object MixedJSONQuery {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("SQL-MixedJSONQuery")
        .master("local[4]")
        .getOrCreate()

    val currentFile = Paths.get(".").toAbsolutePath.toString
    val projectFolder = new File(currentFile).getParent

    val transactions = spark.read.json(s"file://${projectFolder}/src/main/resources/data/mixed.json")
    transactions.printSchema()
    transactions.createOrReplaceTempView("transactions")

    // all
    println("all")
    spark.sql("select id from transactions").show()

    // more columns
    println("more")
    spark.sql("select id, since from transactions").show()

    // deeper
    println("deeper")
    spark.sql("select id, address.zip from transactions").show()

    // select from an array
    println("*** selecting an array valued column")
    spark.sql("select id, orders from transactions").show()

    // select an element from an array
    println("*** selecting a specific array element")
    spark.sql("select id, orders[0] from transactions").show()

  }
}
