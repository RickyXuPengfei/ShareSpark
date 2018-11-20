package DataFrame

import org.apache.spark.sql.SparkSession

object Range {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Range")
      .master("local[2]")
      .getOrCreate()

    // create a DataFrame with a single column
    println("dense range with default partitioning")
    val df1 = spark.range(10,14)
    println(s"Partitions = ${df1.rdd.partitions.length}")

    // stepped range
    println(s"stepped range")
    val df2 = spark.range(10,14,2)
    df2.show()
    println(s"Partitions = ${df2.rdd.partitions.length}")

    // stepped range with specified partitions
    println(s"stepped range with specified partitions")
    val df3 = spark.range(10,14,2, 2)
    df3.show()
    println(s"Partitions = ${df3.rdd.partitions.length}")

    println("\n*** range with just a limit")
    val df4 = spark.range(3)
    df4.show()
    println("# Partitions = " + df4.rdd.partitions.length)
  }
}
