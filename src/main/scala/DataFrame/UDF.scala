package DataFrame

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable


object UDF {
  private case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("UDF")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    // create an RDD with some data
    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    val customerDF = spark.sparkContext.parallelize(custs, 4).toDF()

    // use UDF to construct Column from other Columns
    val mySales =  udf({(sales:Double, disc: Double) => sales - disc})

    println("Use mysales for selecting")
    customerDF.select($"id",mySales($"sales",$"discount").as("After discount")).show()

    // UDF filter
    val myNameFilter = udf({(s:String) => s.startsWith("W")})

    println("*** UDF used for filtering")
    customerDF.filter(myNameFilter($"name")).show()

    def stateRegin  = udf({
      (state:String) => state match {
        case "CA" | "AK" | "OR" | "WA" => "West"
        case "ME" | "NH" | "MA" | "RI" | "CT" | "VT" => "NorthEast"
        case "AZ" | "NM" | "CO" | "UT" => "SouthWest"
      }
    })

    println("*** UDF used for grouping")
    customerDF.groupBy(stateRegin($"state").as("Region")).count().show()
    customerDF.withColumn("Region", stateRegin($"state")).groupBy($"Region").agg(
      count($"Region").as("RegionCount")
    ).show()

    // UDF sorting

    println("*** UDF used for sorting")
    customerDF.orderBy(stateRegin($"state").as("Region"),mySales($"sales",$"discount").as("After discount")).show()

    // literals in UDF calls
    val salesFilter = udf(
      {(s:Double, min:Double) => s>min}
    )
    println("*** UDF with scalar constant parameter")
    customerDF.filter(salesFilter($"sales",lit(2000.0))).show()

    // an array of literals
    // functions.array() = mutable.WrappedArray
    val stateFilter = udf({
      (state:String, regionStates: mutable.WrappedArray[String])  => regionStates.contains(state)
    }
    )

    println("*** UDF with array constant parameter")
    customerDF.filter(stateFilter($"state", array(lit("CA"), lit("MA"), lit("NY"), lit("NJ")))).show()


    // create a struct of literals
    // functions.struct() = Row
    val multipleFilter = udf({
      (state:String, discount:Double, thestruct: Row) =>
        state == thestruct.getString(0) && discount > thestruct.getDouble(1)
    })
    println("*** UDF with array constant parameter")
    customerDF.filter(
      multipleFilter($"state",$"discount",struct(lit("CA"), lit(100.0)))
    ).show()


  }
}
