package SparkSQL

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

object UDAF_Multi {

  private class ScalaAggregateFunc extends  UserDefinedAggregateFunction {
    override def inputSchema: StructType = new StructType().add("num", DoubleType)

    override def bufferSchema: StructType = new StructType()
      .add("rows", LongType)
      .add("count", LongType)
      .add("sum", DoubleType)

    override def dataType: DataType = new StructType()
      .add("rows", LongType)
      .add("count", LongType)
      .add("sum", DoubleType)

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0l
      buffer(1) = 0l
      buffer(2) = 0.0
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getLong(0) + 1
      if (!input.isNullAt(0)){
        buffer(1) = buffer.getLong(1) +1
        buffer(2) = buffer.getDouble(2) + input.getDouble(0)
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
      buffer1(2) = buffer1.getDouble(2) + buffer2.getDouble(2)
    }

    override def evaluate(buffer: Row): Any = {
      Row(
        buffer.getLong(0),
        buffer.getLong(1),
        buffer.getDouble(2)
      )
    }
  }

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("SQL-UDAF_Multi")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    // create an RDD of tuples with some data
    // NOTE the use of Some/None to create a nullable column
    val custs = Seq(
      (1, "Widget Co", Some(120000.00), 0.00, "AZ"),
      (2, "Acme Widgets", Some(800.00), 500.00, "CA"),
      (3, "Widgetry", Some(200.00), 200.00, "CA"),
      (4, "Widgets R Us", None, 0.0, "CA"),
      (5, "Ye Olde Widgete", Some(500.00), 0.0, "MA"),
      (6, "Charlestown Widget", None, 0.0, "MA")
    )
    val customerRows = spark.sparkContext.parallelize(custs, 4)
    val customerDF = customerRows.toDF("id", "name", "sales", "discount", "state")

    val mystats = new ScalaAggregateFunc
    customerDF.printSchema()

    customerDF.createOrReplaceTempView("customers")
    spark.udf.register("stats", mystats)

    val sqlResult = spark.sql(
      s"""
         | select state, stats(sales) as s
         | from customers
         | group by state
       """.stripMargin)
    sqlResult.printSchema()
    sqlResult.show()

    val sqlResult2 = spark.sql(
      s"""
         | select state, s.rows, s.count, s.sum
         | from (
         |  select state, stats(state) as s
         |  from customers
         |  group by state
         | )
       """.stripMargin)
    sqlResult2.printSchema()
    sqlResult2.show()
  }
}
