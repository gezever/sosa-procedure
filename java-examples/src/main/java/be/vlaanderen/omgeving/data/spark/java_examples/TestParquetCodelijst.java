package be.vlaanderen.omgeving.data.spark.java_examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.array_contains;
import static org.apache.spark.sql.functions.array_size;
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
//        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());

        // Read json or parquet into dataframe
        Dataset<Row> datasetcodelijsten = spark.read().parquet("java-examples/src/main/resources/datalake/luchtzuiveringssysteem_v1.parquet");
        datasetcodelijsten.cache();

//        datasetcodelijsten.show();
//        datasetcodelijsten.printSchema();


        Dataset<Row> codelijsten = datasetcodelijsten
                .where(array_contains(col("type"), "http://www.w3.org/2004/02/skos/core#ConceptScheme"))
                .select(
                        col("id"),
                        col("prefLabel")
                );

        // test
        codelijsten.cache();
        codelijsten.show();
        codelijsten.write().mode("overwrite").parquet("/Users/pieter/work/git/gezever/sosa-procedure/java-examples/output/codelijsten/codelijsten/parquet/");

        Dataset<Row> systeemtypes = datasetcodelijsten
                .where(col("inScheme").equalTo(lit("https://data.omgeving.vlaanderen.be/id/conceptscheme/lzs"))
                        .and(array_size(col("narrower")).equalTo(0)))
                .select(
                        col("id"),
                        col("type"),
                        col("inScheme"),
                        col("prefLabel"),
                        col("broader")
                );
        // test
        systeemtypes.cache();
        systeemtypes.show();
        systeemtypes.write().mode("overwrite").parquet("/Users/pieter/work/git/gezever/sosa-procedure/java-examples/output/codelijsten/systeemtypes/parquet/");

        Dataset<Row> parameters = datasetcodelijsten
                .where(col("inScheme").equalTo(lit("https://data.omgeving.vlaanderen.be/id/conceptscheme/lzsp")))
                .withColumn("type", explode(col("type")))
                .where(col("type").equalTo(lit("http://www.w3.org/ns/ssn/Property")))
                .select(
                        col("id"),
                        col("type"),
                        col("inScheme"),
                        col("prefLabel"),
                        col("dct_type"),
                        col("applicableUnit"),
//                      col("relevantQuantityKind"),
                        col("subject"),
                        col("seeAlso")
                );
//                .withColumn("dct_type", explode(col("dct_type")))

        // https://medium.com/@ashwin_kumar_/spark-dataframe-cache-and-persist-explained-019ab2abf20f
        // cache or persist, multiple 'spark actions' on the same rdd.
        // Each time you peform a 'spark action' your lazy 'spark transformations' are executed on each workernode and a value is returned to your driver program. This is the driver program.
        parameters.cache();
//        parameters.persist();

        parameters.show();

        // test write codelijst parameters
        parameters.write().mode("overwrite").parquet("/Users/pieter/work/git/gezever/sosa-procedure/java-examples/output/codelijsten/parameters/parquet/");
        // test select parameters voor bepaald lzstype
        parameters.where(array_contains(col("dct_type"), lit("https://data.omgeving.vlaanderen.be/id/concept/luchtzuiveringssysteem/s1")))
                .select(col("id")).show();
        parameters.where(array_contains(col("dct_type"), lit("https://data.omgeving.vlaanderen.be/id/concept/luchtzuiveringssysteem/s2")))
                .select(col("id")).show();
        parameters.where(array_contains(col("dct_type"), lit("https://data.omgeving.vlaanderen.be/id/concept/luchtzuiveringssysteem/s3")))
                .select(col("id")).show();
        // test select lzstypes voor bepaalde parameter
        List<String> dctType = parameters.filter(col("id").equalTo(lit("https://data.omgeving.vlaanderen.be/id/concept/luchtzuiveringssysteem/eigenschap/drukvaloverhetsysteem")))
                .select(col("dct_type")).first().<String>getList(0);
        System.out.println(dctType.stream().collect(Collectors.joining("|")));


        Dataset<Row> bandbreedtes = datasetcodelijsten
                .where(col("inScheme").equalTo(lit("https://data.omgeving.vlaanderen.be/id/conceptscheme/norm_bandbreedte")))
                .withColumn("type", explode(col("type")))
                .withColumn("dct_type", explode(col("dct_type")))
                .select(
                        col("id"),
                        col("type"),
                        col("inScheme"),
                        col("prefLabel"),
                        col("definition"),
                        col("scopeNote"),
                        col("dct_type"),
                        col("relevantProperty"),
                        col("maxValue"),
                        col("minValue"),
                        col("operatie")
                );

//        bandbreedtes.where(array_except("type", array(lit(""))))

        // test
        bandbreedtes.cache();
        bandbreedtes.write().mode("overwrite").parquet("/Users/pieter/work/git/gezever/sosa-procedure/java-examples/output/codelijsten/bandbreedtes/parquet/");
        bandbreedtes.show();

        spark.stop();
    }
}
