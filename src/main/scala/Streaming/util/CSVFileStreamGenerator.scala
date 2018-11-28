package Streaming.util

import java.io.{File, PrintWriter}
import java.net.URI

import scala.util.Random


class CSVFileStreamGenerator(nFiles: Int, nRecords: Int, betweenFilesMsec: Int)  {
  private  val root = new File(new URI("file://"+File.separator + "tmp" + File.separator + "streamFiles"))
  makeExist(root)

  private  val prep = new File(root.getAbsolutePath() + File.separator + "prep")
  makeExist(prep)

  val dest = new File(root.getAbsolutePath + File.separator + "dest")
  makeExist(dest)

  private def makeExist(dir: File): Unit = {
    dir.mkdir()
  }

  private def writeOutput(f: File): Unit = {
    val p = new PrintWriter(f)
    try {
      for (i <- 1 to nRecords){
        p.println(s"Key_${Random.nextInt}")
      }
    } finally {
      p.close()
    }
  }

  def makeFiles(): Unit = {
    for (n <- 1 to nFiles){
      val f = File.createTempFile("Spark_",".txt", prep)
      writeOutput(f)
      val nf = new File(dest + File.separator + f.getName)
      f.renameTo(nf)
      nf.deleteOnExit()
      Thread.sleep(betweenFilesMsec)
    }
  }
}
