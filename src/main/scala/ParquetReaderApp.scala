import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import scala.io.Source

object ParquetReaderApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("JSON to Parquet Converter")
      .master("local[*]") // Use all available cores
      .getOrCreate()

    // Read JSON file
    val jsonFilePath = "src/main/resources/input.json"
    val schema = StructType(Seq(
      StructField("_id", StringType, nullable = false),
      StructField("_type", StringType, nullable = false),
      StructField("hasFeatureOfInterest", StringType, nullable = false),
      StructField("observedProperty", StringType, nullable = false),
      StructField("result", StructType(Seq(
        StructField("_type", StringType, nullable = false),
        StructField("eenheid", StringType, nullable = true),
        StructField("kwalitatieve_waarde", StringType, nullable = true),
        StructField("numerieke_waarde", DoubleType, nullable = true)
      ))),
      StructField("resultTime", TimestampType, nullable = false)
    ))

    val jsonDF: DataFrame = spark.read
      .schema(schema)
      .option("multiline", "true") // use this if your JSON file spans multiple lines
      .json(jsonFilePath)

    println("== JSON Schema ==")
    jsonDF.printSchema()

    println("== JSON Data Preview ==")
    jsonDF.show(truncate = false)

    // (Optional) Add transformation here if needed
    // val transformedDF = jsonDF.filter("someColumn IS NOT NULL")

    // Write as Parquet
    val parquetOutputPath = "src/main/resources/all_parquet"
    jsonDF.write
      .mode("overwrite") // overwrites if the output exists
      .parquet(parquetOutputPath)

    println(s"Data written to Parquet at: $parquetOutputPath")

    val df: DataFrame = spark.read
      .parquet("src/main/resources/all_parquet")

    println("== Parquet Schema ==")
    df.printSchema()
    println("== Parquet Data Preview ==")
    df.show()

    spark.stop()
  }
}