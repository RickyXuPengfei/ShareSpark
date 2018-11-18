package RDD

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import java.util.Collections
import java.util.stream.Stream

import scala.collection.JavaConversions._


class HashJoiner[K, V](small: Seq[(K, V)]) extends java.io.Serializable {
  val m = mutable.HashMap[K, mutable.ListBuffer[V]]()
  small.foreach({
    case (k, v) =>
      if (m.contains(k)) m(k).add(v)
      else {
        m(k) = mutable.ListBuffer[V](v)
      }
  })

  def joinOnLeft[U](large: RDD[(K, U)]): RDD[(K, (U, V))] = {
    large.flatMap({
      case (k, u) =>
        m.get(k).flatMap(ll => Some(ll.map(v => (k, (u, v))))).getOrElse(mutable.ListBuffer())
    }
    )
  }

}

object HashJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HashJoin").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val smallRDD = sc.parallelize(
      Seq((1, 'a'), (1, 'c'), (2, 'a'), (3, 'x'), (3, 'y'), (4, 'a')),
      4
    )

    val largeRDD = sc.parallelize(
      for (x <- 1 to 100) yield (x % 4, x),
      4
    )

    // simply join
    val joined = largeRDD.join(smallRDD)
    joined.collect().foreach(println)

    // Using HashJoiner
    println("hash join result")
    // smallRDD as broadcastValue
    val joiner = new HashJoiner(smallRDD.collect())
    val hashJoined = joiner.joinOnLeft(largeRDD)
    hashJoined.collect().foreach(println)
  }
}
