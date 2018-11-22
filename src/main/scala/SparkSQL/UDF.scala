package SparkSQL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.functions._



// a case class for our sample table
case class Cust(id: Integer, name: String, sales: Double, discounts: Double, state: String)

// an extra case class to show how UDFs can generate structure
case class SalesDisc(sales: Double, discounts: Double)

object UDF {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("SQL-UDF")
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
    val customerTable = spark.sparkContext.parallelize(custs, 4).toDF()

    val westernState = udf({
      (state:String) => Seq("CA","OR","WA","AK").contains(state)
    })

    customerTable.createOrReplaceTempView("customerTable")
    spark.udf.register("westernState",westernState)

    // WHERE clause
    println("UDF in a WHERE")
    val westernStates = spark.sql("select * from customerTable where westernState(state)")
    westernStates.foreach(println(_))

    // HAVING clause
    val manyCustomers = udf({
      (cnt:Long) => cnt > 2
    })

    spark.udf.register("manyCustomers", manyCustomers)
    println("UDF in a HAVING")
    val stateManyCustomers =
      spark.sql(
        s"""
           | select state, count(id) as custCount
           | from customerTable
           | group by state
           | having manyCustomers(custCount)
         """.stripMargin)
    stateManyCustomers.foreach(println(_))

    // GROUP BY clause
    val stateRegion = udf({
      (state:String) => state match {
        case "CA" | "AK" | "OR" | "WA" => "West"
        case "ME" | "NH" | "MA" | "RI" | "CT" | "VT" => "NorthEast"
        case "AZ" | "NM" | "CO" | "UT" => "SouthWest"
      }
    })
    spark.udf.register("stateRegion", stateRegion)
    println("UDF in GROUP BY")
    val salesByRegion = spark.sql(
      s"""
         | select sum(sales), stateRegion(state) as totalSales
         | from customerTable
         | group by stateRegion(state)
       """.stripMargin)
    salesByRegion.foreach(println(_))

    // apply a UDF to the result columns
    val discountRatio = udf({
      (sales: Double, discounts: Double) => discounts/sales
    })

    spark.udf.register("discountRatio", discountRatio)
    println("UDF in a result")
    val customerDiscounts = spark.sql(
      s"""
         | select id, discountRatio(sales, discounts) as ratio
         | from customerTable
       """.stripMargin)
    customerDiscounts.foreach(println(_))

    // use UDF to create nested structure
    def makeStructFunc(sales: Double, disc:Double): SalesDisc = SalesDisc(sales, disc)
    val makeStruct = udf(
      makeStructFunc _
    )
    spark.udf.register("makeStruct", makeStruct)
    println("UDF creating structed result")
    val withStruct = spark.sql(
      s"""
         | select id, sd.sales
         | from  (select id, makeStruct(sales, discounts) AS sd from customerTable)
       """.stripMargin)
    withStruct.show()
  }
}
