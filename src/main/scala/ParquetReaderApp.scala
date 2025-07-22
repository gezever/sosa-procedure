import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import scala.io.Source

object ParquetReaderApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("JSON to Parquet Converter")
      .master("local[*]") // Use all available cores
      .getOrCreate()

    // Read JSON file
    val jsonFilePath = "src/main/resources/all.json"
    val jsonDF: DataFrame = spark.read
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

    spark.stop()
  }
}