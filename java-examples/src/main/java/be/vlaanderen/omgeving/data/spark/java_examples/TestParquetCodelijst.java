package be.vlaanderen.omgeving.data.spark.java_examples;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.array_contains;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.lit;

/**
 *
 */
public class TestParquetCodelijst {
    public static void main(String[] args) {
        processCodelijst();
    }

    public static void processCodelijst() {
        SparkSession spark = SparkSession.builder().appName("ProcessCodelijst").master("local").getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
        // Read json or parquet into dataframe
        Dataset<Row> codelijst = spark.read().parquet("java-examples/src/main/resources/datalake/luchtzuiveringssysteem.parquet");

//        codelijst.show();
//        codelijst.printSchema();

        Dataset<Row> parameters_tmp = codelijst.where(array_contains(col("type"), "http://www.w3.org/ns/ssn/Property"));
        Dataset<Row> parameters = parameters_tmp.
                select(
                        col("id"),
                        col("type"),
                        col("applicableUnit"),
                        col("subject"),
                        col("dct_type"),
//                        col("relevantQuantityKind"),
                        col("seeAlso"),
                        col("prefLabel")
                )
                .withColumn("type", explode(col("type")))
//                .withColumn("dct_type", explode(col("dct_type")))
                .where(col("type").equalTo(lit("http://www.w3.org/ns/ssn/Property")));
// https://medium.com/@ashwin_kumar_/spark-dataframe-cache-and-persist-explained-019ab2abf20f

        // cache or persist, multiple 'spark actions' on the same rdd.
        // Each time you peform a 'spark action' the lazy 'spark transformations' are executed and a value is returned to your driver program.
        parameters.cache();
//        parameters.persist();
//        Broadcast<Dataset<Row>> broadcast = sparkContext.broadcast(tst);
//        Dataset<Row> parameters = broadcast.getValue();
        parameters.show();

        parameters.write().mode("overwrite").parquet("/Users/pieter/work/git/gezever/sosa-procedure/java-examples/output/codelijsten/parameters/parquet/");

        parameters.where(array_contains(col("dct_type"), lit("https://data.omgeving.vlaanderen.be/id/concept/luchtzuiveringssysteem/s1")))
                .select(col("id")).show();
        parameters.where(array_contains(col("dct_type"), lit("https://data.omgeving.vlaanderen.be/id/concept/luchtzuiveringssysteem/s2")))
                .select(col("id")).show();
        parameters.where(array_contains(col("dct_type"), lit("https://data.omgeving.vlaanderen.be/id/concept/luchtzuiveringssysteem/s3")))
                .select(col("id")).show();

        List<String> dctType = parameters.filter(col("id").equalTo(lit("https://data.omgeving.vlaanderen.be/id/concept/luchtzuiveringssysteem/eigenschap/drukvaloverhetsysteem")))
                .select(col("dct_type")).first().<String>getList(0);

        System.out.println(dctType.stream().collect(Collectors.joining("|")));


        Dataset<Row> bandbreedtes_tmp = codelijst.where(col("inScheme").equalTo(lit("https://data.omgeving.vlaanderen.be/id/conceptscheme/norm_bandbreedte")));
        Dataset<Row> bandbreedtes = bandbreedtes_tmp.
                withColumn("type", explode(col("type")))
                .withColumn("dct_type", explode(col("dct_type")))
                .select(
                        col("id"),
                        col("type"),
                        col("dct_type"),
                        col("inScheme"),
                        col("prefLabel"),
                        col("definition"),
                        col("note"),
                        col("relevantProperty"),
                        col("maxValue"),
                        col("minValue"),
                        col("operatie")
                );

//        bandbreedtes.where(array_except("type", array(lit(""))))

        bandbreedtes.show();

        spark.stop();
    }
}
