package Streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Windowing {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Windowing").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // stream
    val ssc = new StreamingContext(sc, Seconds(1))
    val qm = new QueueMaker(sc, ssc)

    val stream = qm.inputStream

    // register for data
    stream.window(Seconds(5), Seconds(2)).foreachRDD(r => {
      if (r.count() == 0) {
        println("Empty")
      }
      else {
        println(s"Count  = ${r.count()}  min = ${r.min()}  max = ${r.max()}")

      }
    })

    ssc.start()

    new Thread("Delayed Termination") {
      override def run(): Unit = {
        qm.populateQueue()
        Thread.sleep(1500)
        println("stop streaming")
        ssc.stop()
      }
    }.start()

    try {
      ssc.awaitTermination()
      println("streaming terminated")
    } catch {
      case e: Exception => {
        println("*** streaming exception caught in monitor thread")
      }
    }
  }
}
