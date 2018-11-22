package SparkSQL

import java.io.File
import java.nio.file.Paths

import org.apache.spark.sql.SparkSession

object JSONSchemaInference {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("JSONSchemaInference")
        .master("local[4]")
        .getOrCreate()

    val currentFile = Paths.get(".").toAbsolutePath.toString
    val projectFolder = new File(currentFile).getParent

    val ex1 = spark.read.json(s"file://${projectFolder}/src/main/resources/data/inference1.json")
    ex1.schema.printTreeString()
    ex1.createOrReplaceTempView("table1")
    println("simple query")
    spark.sql("select b from table1").foreach(print(_))

    // missing field
    val ex2 = spark.read.json(s"file://${projectFolder}/src/main/resources/data/inference2.json")
    ex2.schema.printTreeString()
    ex2.createOrReplaceTempView("table2")
    println("it's OK to reference a sometimes missing field")
    spark.sql("select b from table2").foreach(println(_))
    println("it's OK to reach into a sometimes-missing field")
    spark.sql("select g.h from table2").foreach(println(_))

    // scalar and struct conflicts
    val ex3 = spark.read.json(s"file://${projectFolder}/src/main/resources/data/inference3.json")
    ex3.schema.printTreeString()
    ex3.createOrReplaceTempView("table3")
    println("it's ok to query conflicting types but not reach inside them")
    // don't try to query g.h or g[1]
    spark.sql("select g from table3").foreach(println(_))
  }
}
