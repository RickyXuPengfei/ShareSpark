package SparkSQL

import java.io.File

import org.apache.spark.sql.SparkSession

object PartitionedTable {
  case class Fact(year: Integer, month: Integer, id: Integer, cat: Integer)

  def main(args: Array[String]): Unit = {
    val exampleRoot = "file:///tmp/ShareSpark"

    utils.PartitionedTableHierarchy.deleteRecursively(new File(exampleRoot))

    val tableRoot = exampleRoot + "/Table"

    val spark =
      SparkSession.builder()
        .appName("PartitionedTable")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    val ids = 1 to 1200
    val facts = ids.map(id => {
      val month = id % 12  + 1
      val year = 2000 + (month % 12)
      val cat  = id % 4
      Fact(year, month, id, cat)
    })

    val factsDF = spark.sparkContext.parallelize(facts, 4).toDF()

    println("*** Here is some of the sample data")
    factsDF.show(20)

    //
    // Register with a table name for SQL queries
    //
    factsDF.createOrReplaceTempView("original")

    spark.sql(
      s"""
         | DROP TABLE IF EXISTS partitioned
       """.stripMargin)

    // create the partitioned table
    spark.sql(
      s"""
         | CREATE TABLE partitioned
         |  (year INTEGER, month INTEGER, id INTEGER, cat INTEGER)
         | USING PARQUET
         | PARTITIONED BY (year, month)
         | LOCATION "${tableRoot}"
       """.stripMargin)

    // insert the data into the partitioned table, relying on dynamic partitioning
    spark.sql(
      s"""
         | INSERT INTO TABLE partitioned
         | SELECT id, cat, year, month FROM original
       """.stripMargin)

    println("*** partitioned table in the file system, after the initial insert")
    utils.PartitionedTableHierarchy.printRecursively(new File(tableRoot))

    // query the partitioned table
    val fromPartitioned = spark.sql(
      s"""
         | SELECT year, count(*)
         | FROM partitioned
         | GROUP BY year
         | ORDER BY year
       """.stripMargin)
    fromPartitioned.show()

    // dynamic insert
    spark.sql(
      s"""
         | INSERT INTO TABLE partitioned
         | VALUES
         |  (1400, 1, 2016, 1),
         |  (1401, 2, 2017, 3)
       """.stripMargin)

    // dynamic insert with PARTITION
    spark.sql(
      s"""
         | INSERT INTO TABLE partitioned
         | PARTITION (year, month)
         | VALUES
         |  (1500, 1, 2016, 2),
         |  (1501, 2, 2017, 4)
       """.stripMargin)

    // static partition insert
    spark.sql(
      s"""
         | INSERT INTO TABLE partitioned
         | PARTITION (year = 2017, month = 7)
         | VALUES
         |  (1600, 1),
         |  (1601, 2)
       """.stripMargin)

    // statically and 'month' dynamic
    spark.sql(
      s"""
         | INSERT INTO TABLE partitioned
         | PARTITION (year = 2017, month)
         | VALUES
         |  (1700, 1, 9),
         |  (1701, 2, 10)
       """.stripMargin)

    println("*** the additional rows that were inserted")


    // Query
    val afterInserts = spark.sql(
      s"""
         | SELECT year, month, id, cat
         | FROM partitioned
         | WHERE year > 2011
         | ORDER BY year, month
       """.stripMargin)

    afterInserts.show()

    println("*** partitioned table in the file system, after all additional inserts")
    utils.PartitionedTableHierarchy.printRecursively(new File(tableRoot))

    println("*** query summary of partitioned table, after additional inserts")

    val finalCheck = spark.sql(
      s"""
         | SELECT year, COUNT(*) as count
         | FROM partitioned
         | GROUP BY year
         | ORDER BY year
      """.stripMargin)

    finalCheck.show()

  }


}
