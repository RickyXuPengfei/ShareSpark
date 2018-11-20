package DataFrame

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

object Select {
  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Select")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )

    val custDF = spark.sparkContext.parallelize(custs,4).toDF()
    custDF.cache()

    // select *
    println("use * to select all columns")
    custDF.select("*").show()

    // select multiple columns
    println("select multiple columns with String*")
    custDF.select("id","discount").show()

    // use apply()
    println("use apply()")
    custDF.select(custDF("id"),custDF("discount")).show()

    // as() to rename
    println("rename column")
    custDF.select(custDF("id").as("Customer ID"),
                  custDF("discount").as("Customer Discount")).show()

    // $ as shorthand to obtain Column
    println("$ as shorthand to obtain Column")
    custDF.select($"id".as("Customer ID"),
                  $"discount".as("Customer Discount")).show()


    // manipulate values
    println("use DSL to manipluate valuse")
    custDF.select(($"discount"*2).as("Double Discount")).show()

    // select * and add more
    custDF.select($"*",$"id".as("New id")).show()

    // use lit()
    println("use lit() to add a literal column")
    custDF.select($"id",functions.lit(42).as("FortyTwo")).show()

    // use array()
    println("use array() to contains multiple columns")
    custDF.select($"id",functions.array($"discount",$"state").as("Staff")).show()

    // use rand
    println("use rand() to add random value")
    custDF.select($"id",functions.rand().as("Random ID")).show()
  }
}
