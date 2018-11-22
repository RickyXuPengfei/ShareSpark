package SparkSQL

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object UDAF {

  private class ScalaAggregateFunc extends UserDefinedAggregateFunction {
    // input type
    override def inputSchema: StructType = StructType(Seq(StructField("sales", DoubleType)))

    // buffer type
    override def bufferSchema: StructType = StructType(Seq(StructField("sumLargeSales", DoubleType)))

    // return type
    override def dataType: DataType = DoubleType

    // always get the same result
    override def deterministic: Boolean = true

    // initial
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0.0
    }

    // update in each partition
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val sum = buffer.getDouble(0)
      if (!input.isNullAt(0)) {
        val sales = input.getDouble(0)
        if (sales > 5000) {
          buffer(0) = sum + sales
        }
      }
    }

    // merge different partitions
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getDouble(0)
    }
  }

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("SQL-UDAF")
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

    val mySum = new ScalaAggregateFunc()
    customerDF.printSchema()

    customerDF.createOrReplaceTempView("customers")
    spark.udf.register("mysum", mySum)

    spark.sql(
      s"""
         | select state, mysum(sales) as bigsales
         | from customers
         | group by state
         """.stripMargin).show()
  }
}
