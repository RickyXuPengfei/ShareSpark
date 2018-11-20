package DataFrame

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Transform {

  private case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("Transform")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    val custDF = spark.sparkContext.parallelize(custs, 4).toDF()

    println("the original DataFrame")
    custDF.show()

    val myFunc = udf({
      (x: Double) => x + 1
    })

    // get an Array of columns then transform them into spark.sql.Column
    val colNames = custDF.columns
    val cols = colNames.map(cName => custDF.col(cName))
    val theColumn = custDF("discount")

    // apply myFunc on theColumn
    val mappedCols = cols.map(c => if (c.toString() == theColumn.toString()) myFunc(c).as("transformed") else c)

    val newDF = custDF.select(mappedCols: _*)
    newDF.show()
  }
}
