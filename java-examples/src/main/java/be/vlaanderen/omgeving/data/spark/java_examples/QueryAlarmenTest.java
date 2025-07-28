package be.vlaanderen.omgeving.data.spark.java_examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
 */
public class QueryAlarmenTest {
    public static void main(String[] args) {
        generatePrimaireAlarmenBySqlQuery();
//        generatePrimaireAlarmenWithDataframeApi();
    }

    private static void generatePrimaireAlarmenBySqlQuery() {
        SparkSession spark = SparkSession.builder().appName("QueryAlarmen").master("local").getOrCreate();
        // Read json into dataframe
        Dataset<Row> lzs_df = spark.read().option("multiline", "true").json("java-examples/src/main/resources/datalake/lzs.json");
        Dataset<Row> normen_df = spark.read().option("multiline", "true").json("java-examples/src/main/resources/datalake/normen.json");

        // Parquet or json files can also be used to create a temporary view and then used in SQL statements
        lzs_df.createOrReplaceTempView("lzsview");
        normen_df.createOrReplaceTempView("normview");

        // Bereken absolute 'norm' bandbreedtes per parameter per lzs
        // Een norm specifieert 'abnormale' bandbreedtes (outofboundranges) voor een parameter.
        // Deze bandbreedtes kunnen relatief t.o.v referentiewaarde uitgedrukt zijn (operatie = 'som' of 'product') of absoluut
        Dataset<Row> absnormbandbreedtes = spark.sql("" +
                                                     "WITH lzs AS (SELECT _id, type FROM lzsview), " +
                                                     "normaloperatingrange AS (SELECT _id as lzsid, explode(parameterconditie) as parameterrange FROM lzsview), " +
                                                     "norm AS (SELECT klasse, parameter, explode(bandbreedtes) as bandbreedte FROM normview) " +
                                                     "SELECT lzs._id as lzsid, " +
                                                     "       norm.parameter, " +
                                                     "       COALESCE(normaloperatingrange.parameterrange.value, normaloperatingrange.parameterrange.minValue, normaloperatingrange.parameterrange.maxValue) as referentiewaarde, " +
                                                     "       norm.bandbreedte.minValue as relatieveMinValueNormBandbreedte, " +
                                                     "       norm.bandbreedte.maxValue as relatieveMaxValueNormBandbreedte, " +
                                                     "       norm.bandbreedte.operatie as operatie, " +
                                                     "       norm.bandbreedte.label, " +
                                                     "       norm.bandbreedte.type, " +
                                                     "       norm.bandbreedte.note, " +
                                                     "       CASE WHEN norm.bandbreedte.operatie = 'som' THEN COALESCE(normaloperatingrange.parameterrange.maxValue + norm.bandbreedte.minValue, normaloperatingrange.parameterrange.value + norm.bandbreedte.minValue) " +
                                                     "            WHEN norm.bandbreedte.operatie = 'product' THEN COALESCE(normaloperatingrange.parameterrange.maxValue * norm.bandbreedte.minValue, normaloperatingrange.parameterrange.value * norm.bandbreedte.minValue) " +
                                                     "            WHEN norm.bandbreedte.operatie is NULL THEN norm.bandbreedte.minValue " +
                                                     "       END as minValue, " +
                                                     "       CASE WHEN norm.bandbreedte.operatie = 'som' THEN COALESCE(normaloperatingrange.parameterrange.minValue + norm.bandbreedte.maxValue, normaloperatingrange.parameterrange.value + norm.bandbreedte.maxValue) " +
                                                     "            WHEN norm.bandbreedte.operatie = 'product' THEN COALESCE(normaloperatingrange.parameterrange.minValue * norm.bandbreedte.maxValue, normaloperatingrange.parameterrange.value * norm.bandbreedte.maxValue) " +
                                                     "            WHEN norm.bandbreedte.operatie is NULL THEN norm.bandbreedte.maxValue " +
                                                     "       END as maxValue " +
                                                     "FROM lzs " +
                                                     "JOIN norm ON lzs.type = norm.klasse " +
                                                     "LEFT JOIN normaloperatingrange ON normaloperatingrange.lzsid = lzs._id AND normaloperatingrange.parameterrange.parameter = norm.parameter");

        absnormbandbreedtes.show();
        absnormbandbreedtes.printSchema();

        absnormbandbreedtes.createOrReplaceTempView("absnormbandbreedtes");
        Dataset<Row> observaties = spark.read().option("multiline", "true").json("java-examples/src/main/resources/datalake/observaties.json");
        observaties.createOrReplaceTempView("observaties");

        Dataset<Row> primairealarmen = spark.sql("SELECT " +
                                                 "               concat('observatie:', uuid()) as _id, " +
                                                 "               'sosa:Observation' as _type, " +
                                                 "               obs.hasFeatureOfInterest, " +
                                                 "               obs.observedProperty, " +
                                                 "               current_date() as resultTime, " +
                                                 "               obs.resultTime as phenomenTime," +
                                                 "               absnorm.type as alarmtype,  " +
                                                 "               absnorm.label,  " +
                                                 "               obs.result.numerieke_waarde as gemetenwaarde, " +
                                                 "               absnorm.referentiewaarde, " +
                                                 "               absnorm.minValue as  minValueNormBandbreedte, " +
                                                 "               absnorm.maxValue as  maxValueNormBandbreedte " +
                                                 "        FROM  observaties obs " +
                                                 "        JOIN  absnormbandbreedtes absnorm ON absnorm.lzsid = obs.hasFeatureOfInterest AND absnorm.parameter = obs.observedProperty " +
                                                 "        WHERE (absnorm.minValue is NULL OR obs.result.numerieke_waarde > absnorm.minValue) AND (absnorm.maxValue IS NULL OR obs.result.numerieke_waarde < absnorm.maxValue)");


        primairealarmen.show();
//        primairealarmen.write().mode("overwrite").parquet("/Users/pieter/work/git/gezever/sosa-procedure/java-examples/target/primairealarmen.parquet");
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
//                .drop(operatingrange.col("_id"))
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
}
