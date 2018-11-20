package DataFrame

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object UDAF {

  private class ScalaAggregateFunction extends UserDefinedAggregateFunction {

    // input field
    override def inputSchema: StructType = new StructType().add(StructField("sales", DoubleType))

    // This is the internal fields you keep for computing your aggregate.
    override def bufferSchema: StructType = new StructType().add(StructField("sumLargeSales", DoubleType))

    // This is the output type of your aggregatation function.
    override def dataType: DataType = DoubleType

    // always gets the same result
    override def deterministic: Boolean = true

    // This is the initial value for your buffer schema.
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0.0
    }

    // This is how to update your buffer schema given an input.
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val sum = buffer.getDouble(0)
      if (!input.isNullAt(0)) {
        val sales = input.getDouble(0)
        if (sales > 500.0) {
          buffer(0) = sales + sum
        }
      }
    }

    // This is how to merge two objects with the bufferSchema type.
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    }

    // This is where you output the final value, given the final value of your bufferSchema.
    override def evaluate(buffer: Row): Any = {
      buffer.getDouble(0)
    }
  }

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("UDAF")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    // create an RDD of tuples with some data
    val custs = Seq(
      (1, "Widget Co", 120000.00, 0.00, "AZ"),
      (2, "Acme Widgets", 410500.00, 500.00, "CA"),
      (3, "Widgetry", 200.00, 200.00, "CA"),
      (4, "Widgets R Us", 410500.00, 0.0, "CA"),
      (5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    val customerRows = spark.sparkContext.parallelize(custs, 4)
    val customerDF = customerRows.toDF("id", "name", "sales", "discount", "state")

    val mysum = new ScalaAggregateFunction
    println("the original data")
    customerDF.printSchema()

    println("data after UDAF")
    val result = customerDF.groupBy($"state").agg(mysum($"sales").as("bigsales"))
    result.printSchema()
    result.show()
  }
}
