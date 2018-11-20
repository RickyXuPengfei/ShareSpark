package DataFrame

import org.apache.avro.generic.GenericData
import org.apache.calcite.avatica.ColumnMetaData
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object ComplexSchema {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("ComplexSchema")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._


    // nested StructType
    val rows1 = Seq(
      Row(1, Row("a","b"), 8.00, Row(1,2)),
      Row(2, Row("c","d"), 9.00, Row(3,4))
    )
    val rows1Rdd = spark.sparkContext.parallelize(rows1, 4)

    val schema1 = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("s1", StructType(
          Seq(
            StructField("x",StringType, true),
            StructField("y",StringType,true)
          )
        ),true),
        StructField("d", DoubleType, true),
        StructField("s2", StructType(
          Seq(
            StructField("u",IntegerType, true),
            StructField("v",IntegerType,true)
          )
        ),true)
      )
    )

    println(s"Position of subfiled 'd' is ${schema1.fieldIndex("d")}")

    val df1= spark.createDataFrame(rows1Rdd,schema1)

    println("Schema with nested struct")
    df1.printSchema()

    println("DataFrame with nested Row")
    df1.show()

    println("Select the column with nested Row at the top level")
    df1.select($"s1").show()

    println("Select deep into the column with nested Row basically")
    df1.select("s1.x").show()

    println("Select deep into the column with getField()")
    df1.select($"s1".getField("x")).show()


    // ArrayType
    val rows2 = Seq(
      Row(1, Row("a", "b"), 8.00, Array(1,2)),
      Row(2, Row("c", "d"), 9.00, Array(3,4,5))
    )

    val rows2Rdd = spark.sparkContext.parallelize(rows2, 4)

    val schema2 = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("s1", StructType(
          Seq(
            StructField("x", StringType, true),
            StructField("y", StringType, true)
          )
        ), true),
        StructField("d", DoubleType, true),
        StructField("a",ArrayType(IntegerType),true)
      )
    )

    val df2 = spark.createDataFrame(rows2Rdd, schema2)

    println("Schema with array")
    df2.printSchema()

    println("DataFrame with array")
    df2.show()

    println("Count elements of each array in the column")
    df2.select($"id", size($"a").as("count")).show()

    println("Explode the array")
    df2.select($"id",explode($"a").as("element")).show()

    println("Apply array_contains func")
    df2.select($"id", array_contains($"a",2).as("has2")).show()

    println("Apply getItem() to get the specific element in array")
    df2.select($"id", $"a".getItem(2).as("SecondItem")).show()

    // MapType
    val rows3 = Seq(
      Row(1,8.00, Map("u"->1,"v" ->2)),
      Row(2,9.00, Map("x"->3, "y"->4, "z" -> 5))
    )
    val rows3Rdd = spark.sparkContext.parallelize(rows3,4)

    val schema3 = StructType(
      Seq(
        StructField("id",IntegerType, true),
        StructField("d", DoubleType,true),
        StructField("m", MapType(StringType, IntegerType), true)
      )
    )

    val df3 = spark.createDataFrame(rows3Rdd, schema3)

    println("Schema with map")
    df3.printSchema()

    println("DataFrame with Map")
    df3.show()

    println("Count elements of each map")
    df3.select($"id",size($"m").as("count")).show()

    println("Explode the map elements")
    df3.select($"id", explode($"m")).show()

    println("Get the element in the Map")
    df3.select($"id",$"m".getItem("u")).show()
  }
}
