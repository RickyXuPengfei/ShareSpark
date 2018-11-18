package RDD

import org.apache.spark.{SparkConf, SparkContext}

object MoreOperations {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MoreOperations").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    // put some data into RDD
    val letters = sc.parallelize('a' to 'z', 8)
    val vowels = Seq('a', 'e', 'i', 'o', 'u')

    val consonants = letters.filter(!vowels.contains(_))
    println(s"There are ${consonants.count()} consonants")

    // use a partial function to transform RDD into different type
    val consonantsAsDigits = letters.collect({
      case c: Char if !vowels.contains(c) => c.asDigit
    })
    consonantsAsDigits.foreach(println)

    // flatMap: the words don't always come out in the
    // right order.
    val words = sc.parallelize(Seq("Hello", "World", "!", "Scala"), 2)
    val chars = words.flatMap(_.iterator)
    val charsToString = chars.map(_.toString).reduce(_ + " " + _)
    println(charsToString)

    // groupby
    val numbers = sc.parallelize(1 to 10, 4)
    val modThreeGroups = numbers.groupBy(_ % 3)
    modThreeGroups.foreach({
      case (m, vals) => println(s"mod 3 = ${m}, count = ${vals.count(_ => true)}")
    })

    // countByValue
    // notice use of Option type
    val mods = modThreeGroups.collect({
      case (m, vals) => vals.count(_ => true)
    }).countByValue()
    println("results found 3 times: " + mods.get(3))
    println("results found 4 times: " + mods.get(4))
    println("results found 7 times: " + mods.get(7))

    // max, min
    println(s"maximu element = ${letters.max()}")

    // first
    println(s"first element = ${letters.first()}")

    // sample: return an RDD
    println(s"random sample without replacement: ")
    val sample = letters.sample(false, 0.25, 42)
    sample.foreach(println)

    // sortBy
    println(s"first element when reversed = ${letters.sortBy(x => x, false).first()}")

    // take, takeSample: return Array
    println("first five letters")
    val firstFive = letters.take(5)
    firstFive.foreach(println)

    println("random five letters without replacement and order")
    val randomsFive = letters.takeSample(withReplacement = false, 5)
    randomsFive.foreach(println)
  }
}
