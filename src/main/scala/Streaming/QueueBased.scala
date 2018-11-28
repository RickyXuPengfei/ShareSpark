package Streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

class QueueMaker(sc: SparkContext, ssc: StreamingContext){
  private val rddQueue = new mutable.Queue[RDD[Int]]()

  val inputStream = ssc.queueStream(rddQueue)

  private var base = 1

  private def makeRDD(): RDD[Int] = {
    val rdd = sc.parallelize(base to base + 99, 4)
    this.base = this.base + 100
    rdd
  }

  def populateQueue(): Unit = {
    for (n <- 1 to 10) {
      rddQueue.enqueue(makeRDD())
    }
  }
}

object QueueBased {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("QueueBasedStreaming").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // stream
    val ssc = new StreamingContext(sc, Seconds(1))
    val qm = new QueueMaker(sc, ssc)

    // create the stream
    val stream = qm.inputStream

    stream.foreachRDD(r => println(r.count()))

    ssc.start()

    new Thread("Streaming Termination Monitor") {
      override def run(): Unit = {
        try {
          ssc.awaitTermination()
        } catch {
          case e : Exception => {
            println("*** streaming exception caught in monitor thread")
            e.printStackTrace()
          }
        }
        println("*** streaming terminated")
      }
    }.start()

    println("*** started termination monitor")

    qm.populateQueue()

    Thread.sleep(15000)

    println("*** stopping streaming")
    ssc.stop()

    Thread.sleep(500)
    println("*** done")
  }
}
