package RDD

import org.apache.spark.{SparkConf, SparkContext}

object SimpleRDD {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SimpleRDD").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    // generate a range and put into an RDD
    val numbers: Range = 1 to 10
    val numbersRDD = sc.parallelize(numbers, 4)
    println("Print each element of the original RDD")
    numbersRDD.foreach(println)

    // map operation on numbers
    val stillAnRDD = numbersRDD.map(_.toDouble / 10)

    // get collect of RDD
    val nowAnArray = stillAnRDD.collect()
    println("Now print each element of the array after collection action")
    nowAnArray.foreach(println)

    // glom get each array in the partitions == > Array(Array(),Array(),....)
    val paritionsArray = stillAnRDD.glom()
    println("We have 4 partitions")
    println(paritionsArray.count())
    paritionsArray.foreach(a => {
      println("Partition contents:" +
        a.foldLeft("")(_+" "+_))
    })
  }
}
