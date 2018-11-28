package Streaming

import org.apache.spark.streaming._
import Streaming.util.CSVFileStreamGenerator
import org.apache.spark.{SparkConf, SparkContext}

object FileBased {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FileBased").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // streams
    val ssc = new StreamingContext(sc, Seconds(1))
    val fm = new CSVFileStreamGenerator(10,100, 500)

    // create the stream
    val stream = ssc.textFileStream(fm.dest.getAbsolutePath)

    stream.foreachRDD(r => println(r.count()))

    ssc.start()

    new Thread("Streaming Termination Monitor") {
      override def run(): Unit = {
        try {
          println("Streaming Termination Monitor")
          ssc.awaitTermination()
        }
        catch {
          case e: Exception => {
            println("*** streaming exception caught in monitor thread")
            e.printStackTrace()
          }
        }
        println("*** streaming terminated")
      }
    }.start()

    println("*** started termination monitor")

    Thread.sleep(200)
    println("*** producing data")

    fm.makeFiles()
    Thread.sleep(1000)

    println("*** stopping streaming")
    ssc.stop()

    Thread.sleep(500)

    println("*** done")
  }
}
