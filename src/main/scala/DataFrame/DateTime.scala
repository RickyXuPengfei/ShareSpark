package DataFrame

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


case class DateVal(id: Int, dt: Date, ts: Timestamp)

object DateTime {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("DateTime")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    val rows = Seq(
      DateVal(
        1,
        Date.valueOf("1999-01-11"),
        Timestamp.valueOf("2011-10-02 09:48:05.123456")
      ),
      DateVal(
        1,
        Date.valueOf("2004-04-14"),
        Timestamp.valueOf("2011-10-02 12:30:00.123456")
      ),
      DateVal(
        1,
        Date.valueOf("2008-12-31"),
        Timestamp.valueOf("2011-10-02 15:00:00.123456")
      )
    )

    val tdf = spark.sparkContext.parallelize(rows, 4).toDF()

    println("DataFrame with both DateType and TimestampType")
    tdf.show()

    // Pull a DateType apart
    // year, month, quarter, weekofyear, dayofyear, dayofmonth
    println("Apply funcs on dt")
    tdf.select($"dt", year($"dt").as("year"),
      quarter($"dt").as("quarter"), month($"dt").as("month"),
      weekofyear($"dt").as("weekOfyear"), dayofyear($"dt").as("dayOfyear"),
      dayofmonth($"dt").as("dayOfmonth")).show()

    // datediff date_add date_sub add_months
    println("Date arithmetic")
    tdf.select($"dt", datediff(current_date(), $"dt").as("datediff"),
      date_add($"dt", 20).as("AddDays"),
      date_sub($"dt", 20).as("SubDays"),
      add_months($"dt", 6)).show()

    // Date truncation
    // trunc(date. format) => 回到月初／年初
    tdf.select($"dt", trunc($"dt","YYYY").as("Year"),
      trunc($"dt", "YY").as("year"),
      trunc($"dt","MM").as("month")).show()

    // Date formatting
    // date_format
    println("Date formatting")
    tdf.select($"dt",date_format($"dt","MMM dd, YYYY").as("FormatedDate")).show()

    // Pull a TimeStamp type apart
    tdf.select($"ts", year($"ts").as("year"),hour($"ts").as("hour")).show()
  }
}
