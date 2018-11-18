package RDD

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object OperationsOnPairRDD {

  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("OperationsOnPairRDD").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val people = Seq(
      (5, "Bob", "Jones", "Canada", 23),
      (7, "Fred", "Smith", "Canada", 18),
      (5, "Robert", "Andrews", "USA", 32)
    )
    val peopleRows = sc.parallelize(people, 4)

    type PayLoad = (String, String, String, Int)

    // get the data into an RDD[Pair[U,V]]
    val pairs: RDD[(Int, PayLoad)] = peopleRows.map({
      case (id: Int, first: String, last: String, country: String, age: Int) => {
        (id, (first, last, country, age))
      }
    })

    // reduceByKey
    def combine(p1: PayLoad, p2: PayLoad): PayLoad = {
      if (p1._4 > p2._4) p1 else p2
    }

    val withMax: RDD[(Int, PayLoad)] = pairs.reduceByKey(combine)
    withMax.collect().foreach(println)

    // aggregateByKey

    // seqOp
    def add(acc: Option[PayLoad], rec: PayLoad): Option[PayLoad] = {
      acc match {
        case None => Some(rec)
        case Some(pre: PayLoad) => if (rec._4 > pre._4) Some(rec) else acc
      }
    }

    // comOp
    def combineAcc(acc1: Option[PayLoad], acc2: Option[PayLoad]): Option[PayLoad] = {
      (acc1, acc2) match {
        case (None, None) => None
        case (None, _) => acc2
        case (_, None) => acc1
        case (Some(p1), Some(p2)) => if (p1._4 > p2._4) acc1 else acc2
      }
    }

    val start: Option[PayLoad] = None

    val withMaxOther: RDD[(Int, Option[PayLoad])] =
      pairs.aggregateByKey(start)(add, combineAcc)

    withMaxOther.collect().foreach(println)
  }
}
