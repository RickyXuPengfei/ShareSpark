package SparkSQL

import java.io.File
import java.nio.file.Paths

import org.apache.spark.sql.SparkSession

object JSON {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("JSON")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    // query flat JSON
    val currentFile = Paths.get(".").toAbsolutePath.toString
    println(currentFile)
    val projectFolder = new File(currentFile).getParent
    println(projectFolder)

    val people = spark.read.json(s"file://${projectFolder}/src/main/resources/data/flat.json")
    println("JSON data")
    people.printSchema()
    people.show()

    people.createOrReplaceTempView("people")
    val young = spark.sql("select firstName, lastName from people where age < 30")
    young.foreach(println(_))

    // nested JSON data
    val peopleAddr = spark.read.json(s"file://${projectFolder}/src/main/resources/data/notFlat.json")
    peopleAddr.printSchema()
    peopleAddr.foreach(println(_))
    peopleAddr.createOrReplaceTempView("peopleAddr")
    val inPA = spark.sql("select firstName, lastName from peopleAddr where address.state = 'PA'")
    inPA.foreach(println(_))

    // interesting characters in field names lead to problems with querying, as Spark SQL
    // has no quoting mechanism for identifiers
    val peopleAddrBad = spark.read.json(s"file://${projectFolder}/src/main/resources/data/notFlatBadFieldName.json")
    peopleAddrBad.printSchema()


    import spark.implicits._
    // read json as RDD(String) => DS
    val lines = spark.sparkContext.textFile(s"file://${projectFolder}/src/main/resources/data/notFlatBadFieldName.json")
    val linesFixed = lines.map(_.replaceAllLiterally("$",""))
    val peopleAddrFixed = spark.read.json(linesFixed.toDS())
    peopleAddrFixed.printSchema()
    peopleAddrFixed.createOrReplaceTempView("peopleAddrFixed")
    val inPAFixed = spark.sql("select firstName, lastName from peopleAddrFixed where address.state = 'PA' ")
    inPAFixed.foreach(println(_))
  }
}
