package be.vlaanderen.omgeving.data.spark.java_examples;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.when;


/**
 * org.apache.spark.sql.functions.
 * https://www.databricks.com/blog/2017/05/24/working-with-nested-data-using-higher-order-functions-in-sql-on-databricks.html
 * https://stackoverflow.com/questions/57595632/how-to-join-using-a-nested-column-in-spark-dataframe
 * https://www.sparkcodehub.com/pyspark/dataframe/join-dataframes-array-column-match#google_vignette
 * https://www.baeldung.com/spark-dataframes
 * https://www.baeldung.com/scala/spark-joining-dataframes
 */
public class QueryPrimaireAlarmen {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
//        readJsonFromVariableIntoDataframe();
//        generatePrimaireAlarmenBySqlQuery();
        generatePrimaireAlarmenWithDataframeApi();
    }

    /**
     * Bereken primaire alarmen op basis van sql query.
     * De query die de absolute bandbreedtes per lzs berekent kan telkens on the fly gebeuren of vooraf reeds gematerialiseerd zijn in bestaande persistente view of tabel.
     */
    private static void generatePrimaireAlarmenBySqlQuery() {
        SparkSession spark = SparkSession.builder().appName("QueryAlarmen").master("local").getOrCreate();
        // Read json into dataframe
        Dataset<Row> lzs_df = spark.read().option("multiline", "true").json("java-examples/src/main/resources/datalake/lzs_alt.json");
        Dataset<Row> normen_df = spark.read().option("multiline", "true").json("java-examples/src/main/resources/datalake/normen_alt.json");

        // Parquet or json files can also be used to create a temporary view and then used in SQL statements
        lzs_df.createOrReplaceTempView("lzsview");
        normen_df.createOrReplaceTempView("normview");

        // Bereken absolute 'norm' bandbreedtes per parameter per lzs
        // Een norm specifieert 'abnormale' bandbreedtes (outofboundranges) voor een parameter.
        // Deze bandbreedtes kunnen relatief t.o.v referentiewaarde uitgedrukt zijn (operatie = 'som' of 'product') of absoluut
        String selectBandbreedtes = """
                WITH lzs AS (SELECT _id, type FROM lzsview), \
                operatingrange AS (SELECT _id as lzsid, explode(hasOperatingRange.hasOperatingProperty) as operatingproperty FROM lzsview) \
                SELECT lzs._id as lzsid, \
                       norm.parameter, \
                       COALESCE(operatingrange.operatingproperty.value, operatingrange.operatingproperty.minValue, operatingrange.operatingproperty.maxValue) as referentiewaarde, \
                       norm.minValue as relatieveMinValueNormBandbreedte, \
                       norm.maxValue as relatieveMaxValueNormBandbreedte, \
                       norm.operatie as operatie, \
                       norm.label, \
                       norm.type, \
                       norm.note, \
                       CASE WHEN norm.operatie = 'som' THEN COALESCE(operatingrange.operatingproperty.maxValue + norm.minValue, operatingrange.operatingproperty.value + norm.minValue) \
                            WHEN norm.operatie = 'product' THEN COALESCE(operatingrange.operatingproperty.maxValue * norm.minValue, operatingrange.operatingproperty.value * norm.minValue) \
                            WHEN norm.operatie is NULL THEN norm.minValue \
                       END as minValue, \
                       CASE WHEN norm.operatie = 'som' THEN COALESCE(operatingrange.operatingproperty.minValue + norm.maxValue, operatingrange.operatingproperty.value + norm.maxValue) \
                            WHEN norm.operatie = 'product' THEN COALESCE(operatingrange.operatingproperty.minValue * norm.maxValue, operatingrange.operatingproperty.value * norm.maxValue) \
                            WHEN norm.operatie is NULL THEN norm.maxValue \
                       END as maxValue \
                FROM lzs \
                JOIN normview as norm ON lzs.type = norm.klasse \
                LEFT JOIN operatingrange ON operatingrange.lzsid = lzs._id AND operatingrange.operatingproperty.parameter = norm.parameter""";
        Dataset<Row> absnormbandbreedtes = spark.sql(selectBandbreedtes);

        absnormbandbreedtes.show();
        absnormbandbreedtes.printSchema();

        absnormbandbreedtes.createOrReplaceTempView("absnormbandbreedtes");
        Dataset<Row> observaties = spark.read().option("multiline", "true").json("java-examples/src/main/resources/datalake/observaties.json");
        observaties.createOrReplaceTempView("observaties");

        String selectAlarmen = """
                SELECT \
                   concat('observatie:', uuid()) as _id, \
                   'sosa:Observation' as _type, \
                   obs.hasFeatureOfInterest, \
                   obs.observedProperty, \
                   current_date() as resultTime, \
                   obs.resultTime as phenomenTime,\
                   absnorm.type as alarmtype,  \
                   absnorm.label,  \
                   obs.result.numerieke_waarde as gemetenwaarde, \
                   absnorm.referentiewaarde, \
                   absnorm.minValue as  minValueNormBandbreedte, \
                   absnorm.maxValue as  maxValueNormBandbreedte \
                FROM  observaties obs \
                JOIN  absnormbandbreedtes absnorm ON absnorm.lzsid = obs.hasFeatureOfInterest AND absnorm.parameter = obs.observedProperty \
                WHERE (absnorm.minValue is NULL OR obs.result.numerieke_waarde > absnorm.minValue) AND (absnorm.maxValue IS NULL OR obs.result.numerieke_waarde < absnorm.maxValue)""";

        Dataset<Row> primairealarmen = spark.sql(selectAlarmen);


        primairealarmen.show();
        primairealarmen.write().mode("overwrite").parquet("/Users/pieter/work/git/gezever/sosa-procedure/java-examples/output/parquet/primairealarmen");
        primairealarmen.write().mode("overwrite").json("/Users/pieter/work/git/gezever/sosa-procedure/java-examples/output/json/primairealarmen");
//        primairealarmen.write().mode("overwrite").json("/Users/pieter/work/git/gezever/sosa-procedure/java-examples/target/primairealarmen.json");

        spark.stop();
    }

    private static void generatePrimaireAlarmenWithDataframeApi() {
        SparkSession spark = SparkSession.builder().appName("QueryAlarmen").master("local").getOrCreate();
        // Read json into dataframe
        Dataset<Row> lzs_df = spark.read().option("multiline", "true").json("java-examples/src/main/resources/datalake/lzs.json");
        Dataset<Row> normen_df = spark.read().option("multiline", "true").json("java-examples/src/main/resources/datalake/normen.json");

        Dataset<Row> lzs = lzs_df.select(col("_id"), col("type"));
        Dataset<Row> operatingrange = lzs_df.select(col("_id"), explode(col("parameterconditie")).alias("parameterrange"));
        Dataset<Row> norm = normen_df.select(col("klasse"), col("parameter"), explode(col("bandbreedtes")).alias("bandbreedte"));

        Dataset<Row> temp = lzs.join(norm, norm.col("klasse").equalTo(lzs.col("type")))
                .join(operatingrange, operatingrange.col("_id").equalTo(lzs.col("_id")).and(operatingrange.col("parameterrange.parameter").equalTo(norm.col("parameter"))), "left")
                .withColumn("minValue",
                        when(col("bandbreedte.operatie").equalTo("som"), coalesce(col("parameterrange.maxValue").$plus(col("bandbreedte.minValue")), col("parameterrange.value").$plus(col("bandbreedte.minValue"))))
                                .when(col("bandbreedte.operatie").equalTo("product"), coalesce(col("parameterrange.maxValue").multiply(col("bandbreedte.minValue")), col("parameterrange.value").multiply(col("bandbreedte.minValue"))))
                                .otherwise(col("bandbreedte.minValue")))
                .withColumn("maxValue",
                        when(col("bandbreedte.operatie").equalTo("som"), coalesce(col("parameterrange.minValue").$plus(col("bandbreedte.maxValue")), col("parameterrange.value").$plus(col("bandbreedte.maxValue"))))
                                .when(col("bandbreedte.operatie").equalTo("product"), coalesce(col("parameterrange.minValue").multiply(col("bandbreedte.maxValue")), col("parameterrange.value").multiply(col("bandbreedte.maxValue"))))
                                .otherwise(col("bandbreedte.maxValue")))
                .select(lzs.col("_id").alias("lzsid"),
                        col("parameter"),
                        coalesce(col("parameterrange.value"), col("parameterrange.minValue"), col("parameterrange.maxValue")).alias("referentiewaarde"),
                        col("bandbreedte.minValue").alias("relatieveMinValueNormBandbreedte"),
                        col("bandbreedte.maxValue").alias("relatieveMaxValueNormBandbreedte"),
                        col("bandbreedte.operatie").alias("operatie"),
                        col("bandbreedte.label").alias("label"),
                        col("bandbreedte.type").alias("type"),
                        col("bandbreedte.note").alias("note"),
                        col("minValue"),
                        col("maxValue"));


        temp.show();
        temp.printSchema();

        Dataset<Row> observaties = spark.read().option("multiline", "true").json("java-examples/src/main/resources/datalake/observaties.json");
        observaties.join(temp, col("lzsid").equalTo(col("hasFeatureOfInterest")).and(col("parameter").equalTo(col("observedProperty"))))
                .where(col("minValue").isNull().or(col("result.numerieke_waarde").gt(col("minValue"))).and(col("maxValue").isNull().or(col("result.numerieke_waarde").lt(col("maxValue")))))
                .show();


        spark.stop();
    }

    /*
     * https://stackoverflow.com/questions/57365802/convert-a-list-of-map-in-java-to-dataset-in-spark
     * https://www.baeldung.com/spark-dataframes
     * https://stackoverflow.com/questions/46317098/how-to-convert-map-to-dataframe/46317244
     * https://kontext.tech/project/spark/article/scala-parse-json-string-as-spark-dataframe
     * https://tsaiprabhanj.medium.com/spark-functions-from-json-3ec3fedfd875
     */
    private static void readJsonFromVariableIntoDataframe() {
        SparkSession spark = SparkSession.builder().appName("readjsonvariableintodf").master("local").getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
        String json = """
                {
                    "@context": null,
                    "@graph": [
                        {
                            "_id": "1",
                            "_type" : null
                        },
                        {
                            "_id": "2",
                            "_type" : null
                        }
                    ]
                }
                """;
        try {
            Map object = mapper.readValue(json, Map.class);
            List<Map> records = (List<Map>) object.get("@graph");

            List<String> jsonRecords = new ArrayList<>();
            for (Map record : records) {
                jsonRecords.add(mapper.writeValueAsString(record));
            }

            // alternative 1
            JavaRDD<String> stringDataset = sparkContext.parallelize(jsonRecords);
            Dataset<Row> structuredDs1 = spark.read().json(stringDataset);

            structuredDs1.show();
            structuredDs1.printSchema();

            // alternative 2
            Dataset stringDataset2 = spark.createDataset(jsonRecords, Encoders.STRING());
            stringDataset2.show();
            stringDataset2.printSchema();

            Dataset structuredDs2 = spark.read().json(stringDataset2);
            structuredDs2.show();
            structuredDs2.printSchema();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // todo
    private static void readFromClassPath() {
    }

    // todo
    private static void writeTempFileToLoadDataframe() {
    }
}
