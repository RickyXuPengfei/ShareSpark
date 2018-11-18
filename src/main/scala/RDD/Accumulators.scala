package RDD

import java.util

import org.apache.spark.util.AccumulatorV2
import java.util.Collections

import scala.collection.JavaConversions._


import org.apache.spark.{SparkConf, SparkContext}

object Accumulators {

  class StringSetAccumulator extends AccumulatorV2[String, java.util.Set[String]] {
    private val _set = Collections.synchronizedSet(new util.HashSet[String] {})

    override def isZero: Boolean = _set.isEmpty

    override def copy(): AccumulatorV2[String, java.util.Set[String]] = {
      val newAcc = new StringSetAccumulator()
      newAcc._set.addAll(_set)
      newAcc
    }

    override def reset(): Unit = _set.clear()

    override def add(v: String): Unit = _set.add(v)

    override def value: java.util.Set[String] = _set

    override def merge(other: AccumulatorV2[String, util.Set[String]]): Unit = {
      _set.addAll(other.value)
    }
  }

  //  Use a HashMap from Char to Int as an accumulator.
  class CharCountingAccumulator extends AccumulatorV2[Char, java.util.Map[Char, Int]] {
    private val _map = Collections.synchronizedMap(new util.HashMap[Char, Int]())

    override def isZero: Boolean = _map.isEmpty

    override def copy(): AccumulatorV2[Char, util.Map[Char, Int]] = {
      val newAcc = new CharCountingAccumulator()
      newAcc._map.putAll(_map)
      newAcc
    }

    override def reset(): Unit = _map.clear()

    override def add(v: Char): Unit = {
      _map(v) = (if (_map.contains(v)) _map(v) else 0) + 1
    }

    override def value: util.Map[Char, Int] = _map

    override def merge(other: AccumulatorV2[Char, util.Map[Char, Int]]): Unit = {
      other.value.foreach({
        case (key, count) =>
          _map(key) = (if (_map.contains(key)) _map(key) else 0) + count
      })
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Accumulators").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val words = sc.parallelize(Seq("Fred", "Bob", "Francis",
      "James", "Frederick", "Frank", "Joseph"), 4)

    // an efficient counter -- this is one of the basic accumulators
    val count = sc.longAccumulator
    println("Using a simple counter")
    words.filter(_.startsWith("F")).foreach(n => count.add(1))

    // Accumulate the set of words starting with "F"
    val names = new StringSetAccumulator
    sc.register(names)

    println("Using a set accumulator")
    words.filter(_.startsWith("F")).foreach(n => names.add(n))
    println("All the names starting with 'F' are a set")
    names.value.iterator().foreach(println)

    // Accumulate a map from starting letter to cword count, and
    // extract the count for "F"
    println("Using a hash map accumulator")
    val counts = new CharCountingAccumulator
    sc.register(counts)
    words.foreach(w => counts.add(w.charAt(0)))
    counts.value.iterator.foreach(println)
  }

}
