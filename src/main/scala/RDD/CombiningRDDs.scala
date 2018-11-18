package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, SparkException}

import scala.collection.mutable.ListBuffer

object CombiningRDDs {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("CombiningRDDs").setMaster("local[2]")
    val sc = new SparkContext(sparkconf)

    // put data into RDD
    val letters = sc.parallelize('a' to 'z', 8)
    val vowels = sc.parallelize(Seq('a', 'e', 'i', 'o', 'u'))

    // subtract
    val consonants = letters.subtract(vowels)
    println(s"There are ${consonants.count()} consonants")

    val vowelsNotLetters = vowels.subtract(letters)
    println(s"There are ${vowelsNotLetters.count()} vowels that aren't letters")

    // union
    val lettersAgain = consonants ++ vowels
    println(s"There are ${lettersAgain.count()} letters")

    // union with duplicates, removed
    val tooManyVowels = vowels ++ vowels
    println(s"There are't really ${tooManyVowels.count()} vowels")
    val duplicatedVowels = tooManyVowels.distinct()
    println(s"There are actually ${duplicatedVowels.count()} vowels")

    // intersection
    val earlyLetters = sc.parallelize('a' to 'l', 2)
    val earlyVowels = earlyLetters.intersection(vowels)
    println("There early vowerls:")
    earlyVowels.foreach(println)

    // cartesian
    val numbers = sc.parallelize(1 to 2, 2)
    val cp = vowels.cartesian(numbers)
    println(s"Product has ${cp.count()} elements")

    // index the letters , not sorted by index
    val indexed = letters.zipWithIndex()
    println("indexed letters")
    indexed.foreach {
      case (c, i) => println(s"${i}: ${c}")
    }

    // zip letters and numbers with the same partition count
    val twentySix = sc.parallelize(101 to 126, 8)
    val differentlyIndexed = letters.zip(twentySix)
    differentlyIndexed.foreach({
      case (c, i) => println(s"${i}: ${c}")
    })

    // zip two parts with the different partition count
    val twentySixBadPart = sc.parallelize(101 to 126, 3)
    val cantGet = letters.zip(twentySix)
    try {
      cantGet.foreach({
        case (c, i) => println(s"${i}: ${c}")
      })
    } catch {
      case (e: Exception) => println(s"Exception caught: ${e.getMessage}")
    }

    // zip tow parts with same partition
    val unequalCount = earlyLetters.zip(numbers)
    try {
      unequalCount.foreach({
        case (c, i) => println(s"${i}: ${c}")
      })
    } catch {
      case (se: SparkException) => println(s"Exception caught: ${se.getMessage}")
    }


    // more control for zip
    def zipFunc(lIter: Iterator[Char], nIter: Iterator[Int]): Iterator[(Char, Int)] = {
      val res = new ListBuffer[(Char, Int)]
      while (lIter.hasNext || nIter.hasNext) {
        if (lIter.hasNext && nIter.hasNext) {
          res += ((lIter.next(), nIter.next()))
        } else if (lIter.hasNext) {
          res += ((lIter.next(), 0))
        } else if (nIter.hasNext) {
          res += ((' ', nIter.next()))
        }
      }
      res.iterator
    }

    // zipPartitions函数将多个RDD按照partition组合成为新的RDD，该函数需要组合的RDD具有相同的分区数
    val unequalOK = earlyLetters.zipPartitions(numbers)(zipFunc)
    println("this may not be what you excepted with unequal length RDDs")
    unequalOK.foreach({
      case (c, i) => println(s"${i}: ${c}")
    })
  }
}
