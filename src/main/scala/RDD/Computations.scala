package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object Computations {
  //
  // utilities for printing out a dependency tree
  private def showDep[T](r: RDD[T], depth: Int): Unit = {
    println(s"${depth}  RDD id = ${r.id}")
    r.dependencies.foreach(dep => {
      showDep(dep.rdd, depth + 1)
    })
  }

  def showDep[T](r: RDD[T]): Unit = {
    showDep(r, 0)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Computations").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    // generate some data
    val numberRange: Range = 1 to 10
    val numbers = sc.parallelize(numberRange)
    val bigger = numbers.map(_ * 100)
    val biggerStill = bigger.map(_ + 1)

    println("Debug info for the RDD 'biggerStill'")
    println(biggerStill.toDebugString)

    val s = biggerStill.reduce(_ + _)

    println(s"sum = ${s}")

    println("IDs of the various RDDs")
    println(s"numbers: id = ${numbers.id}")
    println(s"bigger: id = ${bigger.id}")
    println(s"biggerStill: id = ${biggerStill.id}")

    println("depencies working back from RDD 'biggerStill'")
    showDep(biggerStill)

    val moreNumbers = bigger ++ biggerStill
    println("The RDD 'moreNumbers' has more complex dependencies")
    println(moreNumbers.toDebugString)
    println(s"moreNumber: id = ${moreNumbers.id}")
    showDep(moreNumbers)

    moreNumbers.cache()
    println("cached id: the dependencies doesn't change")
    println(moreNumbers.toDebugString)
    showDep(moreNumbers)

    println(s"has RDD 'moreNumbers' been checked : ${moreNumbers.isCheckpointed}")

    // set checkpoint dir and checkpoint moreNumbers
    sc.setCheckpointDir("file:///usr/local/spark/mycode/tmp/checkpoint")
    moreNumbers.checkpoint()

    // check status of checkpoint for moreNumbers
    println(s"has RDD 'moreNumbers' been checked : ${moreNumbers.isCheckpointed}")
    moreNumbers.count()
    println("NOW has it been checkpoint? : " + moreNumbers.isCheckpointed)
    println(moreNumbers.toDebugString)
    // dependencies will change after checkpoint
    showDep(moreNumbers)

    println("this can't throws an exception")
    val thisWillBlowUp = numbers.map {
      case (7) => {
        throw new Exception
      }
      case (n) => n
    }

    println("the exception should get thrown up")
    try {
      println(thisWillBlowUp.count())
    } catch {
      case (e: Exception) => println("Yep, it blew up now")
    }
  }
}
