package DataFrame

import org.apache.spark.sql.{Column, SparkSession, functions}

object GroupingAndAggrgegation {
  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("GroupingAndAggregation")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    // create a sequence of case class objects
    // (we defined the case class above)
    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    // make it an RDD and convert to a DataFrame
    val custDF = spark.sparkContext.parallelize(custs, 4).toDF()
    custDF.cache()
    // groupBy() produces a GroupedData, and you can't do much with
    // one of those other than aggregate it

    // basic form of aggregation assigns a function to
    // each non-grouped column -- you map each column you want
    // aggexpr : "columnName" -> "func"
    println("basic form of aggregation")
    custDF.groupBy("state").agg("discount" -> "max","state" -> "count").show()

    // without grouping column
    println("Without grouping column")
    spark.conf.set("spark.sql.retainGroupColumns","false")
    custDF.groupBy("state").agg("discount" -> "max").show()

    // $ shorthand to state column
    // aggfunc($"columnName")
    println("Column based aggregation")
    custDF.groupBy($"state").agg(functions.max($"discount")).show()

    // plus grouping columns
    println("Column base aggregation plus grouping columns")
    custDF.groupBy("state").agg($"state",functions.max($"discount")).show()

    // use a user-defined function
    def stddevFunc(c: Column): Column = {
      functions.sqrt(functions.avg(c * c) - (functions.avg(c) * functions.avg(c)))
    }

    println("Apply an udf on column")
    custDF.groupBy("state").agg($"state",stddevFunc($"discount").as("Discount Std")).show()

    // aggregate all columns
    println("Aggregation all columns")
    custDF.groupBy("state").count().show()
  }
}
