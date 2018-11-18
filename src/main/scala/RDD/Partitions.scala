package RDD

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object Partitions {
  // utilize for looking at partitioning of an RDD
  def analyze[T](r: RDD[T]): Unit = {
    val partitions = r.glom()
    println(s"${partitions.count()} partitions")

    // zipWithIndex to see the index if each partition
    partitions.zipWithIndex().collect().foreach({
      case (a, i) => println(s"Partition ${i}  contents: ${a.foldLeft("")(_ + " " + _)}")
    })
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Partitions").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    // look at the distribution of numbers across partitions
    val numbers = sc.parallelize(1 to 100, 4)
    println("original RDD: ")
    analyze(numbers)

    val some = numbers.filter(_ < 34)
    println("filtered RDD")
    analyze(some)

    // subtract doesn't do what you might hope
    val diff = numbers.subtract(some)
    println("the complement:")
    analyze(diff)
    println(s"it is a ${diff.getClass.getCanonicalName}")

    // setting the same number of partitions
    val diffSamePart = numbers.subtract(some, 4)
    println("the complement (same number of partitions)")
    analyze(diffSamePart)

    // setting the different number of partitions
    val diffMorePart = numbers.subtract(some, 6)
    println("the complement (different partitions)")
    analyze(diffMorePart)
    println(s"it is a ${diffMorePart.getClass.getCanonicalName}")

    // communications
    def subtracFunc(wholeIter: Iterator[Int], partIter: Iterator[Int]): Iterator[Int] = {
      val partSet = new mutable.HashSet[Int]()
      partSet ++= partIter
      wholeIter.filterNot(partSet.contains(_))
    }

    val diffOriginalPart = numbers.zipPartitions(some)(subtracFunc)
    println("complement with original partitioning")
    analyze(diffOriginalPart)
    println("it is a " + diffOriginalPart.getClass.getCanonicalName)

    // repartition = coalesce(shuffle = true)
    val threePart = numbers.repartition(3)
    println("numbers in three partitions")
    analyze(threePart)
    println(s"it is a ${threePart.getClass.getCanonicalName}")

    // coalesce
    // default partitioner = HashPartitioner
    val twoPart = some.coalesce(2, true)
    println("subset in two partitions after a shuffle")
    analyze(twoPart)
    println("it is a " + twoPart.getClass.getCanonicalName)

    val twoPartNoShuffle = some.coalesce(2, false)
    println("subset in two partitions without a shuffle")
    analyze(twoPartNoShuffle)
    println("it is a " + twoPartNoShuffle.getClass.getCanonicalName)

    // user defined partitioner
    class UserPartitioner(numParts: Int) extends Partitioner {
      override def numPartitions: Int = numParts

      override def getPartition(key: Any): Int = {
        key match {
          case i: Int => i % 10
          case _ => key.toString.toInt % 10
        }
      }
    }

    //    val someUdPartition = numbers.partitionBy(new UserPartitioner(3))
    //    println("subset in two partitions without a shuffle")

    // groupBY
    val groupedNumbers = numbers.groupBy(n => if (n % 2 == 0) "even" else "odd")
    println("numbers grouped into 'odd' and 'even'")
    analyze(groupedNumbers)
    println("it is a " + groupedNumbers.getClass.getCanonicalName)

    // preferredLocations
    numbers.partitions.foreach(p => {
      println("Partition: " + p.index)
      numbers.preferredLocations(p).foreach(s => println("  Location: " + s))
    })

    // mapPartitions
    val pairs = sc.parallelize(for (x <- 1 to 6; y <- 1 to x) yield (("S" + x, y)), 4)
    analyze(pairs)

    val rollup = pairs.foldByKey(0, 4)(_ + _)
    println("just rolling it up")
    analyze(rollup)

    def rollupFunc(i: Iterator[(String, Int)]): Iterator[(String, Int)] = {
      val hashMap = new mutable.HashMap[String, Int]()
      i.foreach({
        case (s, i) => if (hashMap.contains(s)) hashMap(s) = hashMap(s) + i else hashMap(s) = i
      }
      )
      hashMap.iterator
    }

    val inPlaceRollup = pairs.mapPartitions(rollupFunc, true)
    println("rolling it up really carefully")
    analyze(inPlaceRollup)
  }
}
