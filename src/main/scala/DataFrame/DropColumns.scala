package DataFrame

import org.apache.spark.sql.SparkSession


object DropColumns {

  case class Person(id:Int, name:String, sales:Double, discount:Double, state:String)

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("DropColumns")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    val custs = Seq(
      (1, "Widget Co", 120000.00, 0.00, "AZ"),
      (2, "Acme Widgets", 410500.00, 500.00, "CA"),
      (3, "Widgetry", 410500.00, 200.00, "CA"),
      (4, "Widgets R Us", 410500.00, 0.0, "CA"),
      (5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    ).map(row =>Person(row._1,row._2,row._3,row._4,row._5))

    val custDF = spark.sparkContext.parallelize(custs,4).toDF("id","name","sales","discount","state")

    custDF.printSchema()
    custDF.show()

    // remove a couple of columns
    println("Remove a couple of columns")
    val fewerCols = custDF.drop("sales","discount")
    println("fewer columns")

    fewerCols.printSchema()
    fewerCols.show()
  }
}
