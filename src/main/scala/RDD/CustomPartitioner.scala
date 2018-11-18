package RDD

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

class SpecialPartitioner extends Partitioner {
  override def numPartitions: Int = 10

  override def getPartition(key: Any): Int = {
    key match {
      case (x, y: Int, z) => y % numPartitions
      case _ => throw new ClassCastException
    }
  }
}

object CustomPartitioner {
  def analyze[T](r: RDD[T]): Unit = {
    val paritions = r.glom()
    println(paritions.count() + " partitions")


    // use collect() to see them in order
    paritions.zipWithIndex().collect().foreach({
      case (a, i) =>
        println("Partition " + i + " contents (count " + a.count(_ => true) + "):" +
          a.foldLeft("")((e, s) => e + " " + s))
    })
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CustomPartitioner").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val triplets =
      for (x <- 1 to 3; y <- 1 to 20; z <- 'a' to 'd')
        yield ((x, y, z), x * y)


    // Spark has the good sense to use the first tuple element
    val defaultRDD = sc.parallelize(triplets, 10)
    println("with default partitioning")
    analyze(defaultRDD)

    // use SpecialPartitioner
    val deliberateRDD = defaultRDD.partitionBy(new SpecialPartitioner())
    println("with deliberate partitioning")
    analyze(deliberateRDD)

  }
}
