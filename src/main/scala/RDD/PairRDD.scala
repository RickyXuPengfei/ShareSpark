package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

class KeyPartitioner(np:Int) extends Partitioner{
  override def numPartitions: Int = np

  override def getPartition(key: Any): Int = {
    key match {
      case k:Int => k % numPartitions
      case _ => throw new ClassCastException
    }
  }
}

object PairRDD {
  def analyze[T](r: RDD[T]) : Unit = {
    val partitions = r.glom()
    println(partitions.count() + " partitions")

    // use zipWithIndex() to see the index of each partition
    // we need to loop sequentially so we can see them in order: use collect()
    partitions.zipWithIndex().collect().foreach {
      case (a, i) => {
        println("Partition " + i + " contents (count " + a.count(_ => true) + "):" +
          a.foldLeft("")((e, s) => e + " " + s))
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PairRDD").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val pairs = Seq((1,9), (1,2), (1,1), (2,3), (2,4), (3,1), (3,5), (6,2), (6,1), (6,4), (8,1))

    val pairsRDD = sc.parallelize(pairs)

    val reduceRDD = pairsRDD.reduceByKey(Math.min(_,_))

    analyze(pairsRDD)
    analyze(reduceRDD)

    val keyPartitionedPairs = pairsRDD.partitionBy(new KeyPartitioner(4))
    analyze(keyPartitionedPairs)

    // choose the min pair in each partition independently
    def minValFunc(i: Iterator[(Int, Int)]) : Iterator[(Int, Int)] = {
      val m = new mutable.HashMap[Int, Int]()
      i.foreach({
        case (k,v) => if (m.contains(k)) m(k) = Math.min(m(k),v) else m(k) = v
      })
      m.iterator
    }

    val reduceInPlace = keyPartitionedPairs.mapPartitions(minValFunc)

    analyze(reduceInPlace)

    // aggregateByKey
    val reducedRDD2 = pairsRDD.aggregateByKey(Int.MaxValue)(Math.min(_,_), Math.min(_,_))
    analyze(reducedRDD2)
  }
}
