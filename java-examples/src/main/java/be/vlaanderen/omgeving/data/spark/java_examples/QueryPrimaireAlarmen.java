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
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.uuid;
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
        generatePrimaireAlarmenBySqlQuery();
//        generatePrimaireAlarmenWithDataframeApi();
    }

    /**
     * Bereken primaire alarmen op basis van SPARK sql query.
     * De query die de absolute bandbreedtes per lzs berekent kan telkens on the fly gebeuren of vooraf reeds gematerialiseerd zijn in bestaande persistente view, tabel en/of parquet files.
     */
    private static void generatePrimaireAlarmenBySqlQuery() {
        SparkSession spark = SparkSession.builder().appName("QueryAlarmen").master("local").getOrCreate();
        // Read json or parquet into dataframe
        Dataset<Row> lzsDF = spark.read().option("multiline", "true").json("java-examples/src/main/resources/datalake/lzs.json");
        Dataset<Row> normbandbreedtesDF = spark.read().option("multiline", "true").json("java-examples/src/main/resources/datalake/normbandbreedtes.json");

        // Json or Parquet can also be used to create a temporary view and then used in SQL statements
        lzsDF.createOrReplaceTempView("lzsView");
        normbandbreedtesDF.createOrReplaceTempView("normbandbreedtesView");

        // Bereken absolute 'norm' bandbreedtes per parameter per lzs
        // De codelijst met norm bandbreedtes bevat bandbreedtes van abnormale parameter waarden (outofboundranges).
        // Deze bandbreedtes kunnen relatief t.o.v referentiewaarde uitgedrukt zijn (operatie = 'som' of 'product') of absoluut
        String selectBandbreedtes = """
                WITH lzs AS (SELECT _id, type FROM lzsView), \
                operatingrange AS (SELECT _id as lzsid, explode(hasOperatingRange.hasOperatingProperty) as operatingproperty FROM lzsView) \
                SELECT lzs._id as lzsid, \
                       bandbreedte.id as bbid,
                       bandbreedte.forProperty as parameter, \
                       COALESCE(operatingrange.operatingproperty.value, operatingrange.operatingproperty.minValue, operatingrange.operatingproperty.maxValue) as referentiewaarde, \
                       bandbreedte.minValue as minValueNormBandbreedte, \
                       bandbreedte.maxValue as maxValueNormBandbreedte, \
                       bandbreedte.operatie as operatie, \
                       bandbreedte.dc_type as type, \
                       bandbreedte.prefLabel as label, \
                       bandbreedte.scopeNote, \
                       CASE WHEN bandbreedte.operatie = 'som' THEN COALESCE(operatingrange.operatingproperty.maxValue + bandbreedte.minValue, operatingrange.operatingproperty.value + bandbreedte.minValue) \
                            WHEN bandbreedte.operatie = 'product' THEN COALESCE(operatingrange.operatingproperty.maxValue * bandbreedte.minValue, operatingrange.operatingproperty.value * bandbreedte.minValue) \
                            WHEN bandbreedte.operatie is NULL THEN bandbreedte.minValue \
                       END as minValue, \
                       CASE WHEN bandbreedte.operatie = 'som' THEN COALESCE(operatingrange.operatingproperty.minValue + bandbreedte.maxValue, operatingrange.operatingproperty.value + bandbreedte.maxValue) \
                            WHEN bandbreedte.operatie = 'product' THEN COALESCE(operatingrange.operatingproperty.minValue * bandbreedte.maxValue, operatingrange.operatingproperty.value * bandbreedte.maxValue) \
                            WHEN bandbreedte.operatie is NULL THEN bandbreedte.maxValue \
                       END as maxValue \
                FROM lzs \
                JOIN normbandbreedtesView as bandbreedte ON lzs.type = bandbreedte.subject \
                LEFT JOIN operatingrange ON operatingrange.lzsid = lzs._id AND operatingrange.operatingproperty.forProperty = bandbreedte.forProperty""";

        Dataset<Row> absolutenormbandbreedtes = spark.sql(selectBandbreedtes);

        // cache or persist, multiple 'spark actions' on the same rdd.
        // Each time you peform a 'spark action' your lazy 'spark transformations' are executed on each workernode and a value is returned to your driver program. This is the driver program.
        absolutenormbandbreedtes.cache();
        absolutenormbandbreedtes.show();
        absolutenormbandbreedtes.printSchema();

        // Create a temporary view van de absolutenormbandbreedtes, Dit zou ook vooraf gematerialiseerd kunnen zijn in parquet files en/of table.
        absolutenormbandbreedtes.createOrReplaceTempView("absolutenormbandbreedtes");

        // Read en parameter observations die we willen valideren
        Dataset<Row> observaties = spark.read().option("multiline", "true").json("java-examples/src/main/resources/datalake/observaties.json");
        observaties.createOrReplaceTempView("observaties");

        // Een testje ter illustratie: selecteer observaties met abnormale waarden en wat extra metadata
        String selectAlarmenTest = """
                SELECT \
                   uuid() as _id, \
                   obs.hasFeatureOfInterest, \
                   obs.observedProperty, \
                   current_date() as resultTime, \
                   obs.resultTime as phenomenTime,
                   obs.result.numerieke_waarde as gemetenwaarde, \
                   absnorm.referentiewaarde, \
                   absnorm.scopeNote as bandbreedte,  \
                   absnorm.minValue as  minValue, \
                   absnorm.maxValue as  maxValue, \
                   date(obs.resultTime) as datum
                FROM  observaties obs \
                JOIN  absolutenormbandbreedtes absnorm ON absnorm.lzsid = obs.hasFeatureOfInterest AND absnorm.parameter = obs.observedProperty \
                WHERE (absnorm.minValue is NULL OR obs.result.numerieke_waarde > absnorm.minValue) AND (absnorm.maxValue IS NULL OR obs.result.numerieke_waarde < absnorm.maxValue)""";

        Dataset<Row> primairealarmenTest = spark.sql(selectAlarmenTest);
        primairealarmenTest.show();

        // Bereken en creeer primaire alarm observaties op basis van SQL query demo 1
        String selectPrimaireAlarmObservatiesDemo1 = """
                SELECT \
                   concat('observatie:', uuid()) as _id, \
                   'sosa:Observation' as _type, \
                   obs.hasFeatureOfInterest, \
                   obs.observedProperty, \
                   current_timestamp() as resultTime, \
                   obs.resultTime as phenomenonTime,
                   obs._id as wasInformedBy,
                   named_struct( "_id", concat('observatie:',uuid()),
                                 "_type", array('sosa:Result', 'qb:Observation'),
                                 "value", absnorm.bbid,
                                 "label", absnorm.type,
                                 "parameterDimensie", obs.observedProperty,
                                 "lzsDimensie", obs.hasFeatureOfInterest,
                                 "refPeriod",obs.resultTime,
                                 "parametervalue", obs.result.numerieke_waarde,
                                 "parameterunit", obs.result.eenheid,
                                 "scopenote", absnorm.scopeNote
                               ) as result,
                   date(obs.resultTime) as datum
                FROM  observaties obs \
                JOIN  absolutenormbandbreedtes absnorm ON absnorm.lzsid = obs.hasFeatureOfInterest AND absnorm.parameter = obs.observedProperty \
                WHERE (absnorm.minValue is NULL OR obs.result.numerieke_waarde > absnorm.minValue) AND (absnorm.maxValue IS NULL OR obs.result.numerieke_waarde < absnorm.maxValue)""";

        Dataset<Row> primairealarmenDemo1 = spark.sql(selectPrimaireAlarmObservatiesDemo1);

        // cache or persist, multiple 'spark actions' on the same rdd.
        // Each time you peform a 'spark action' your lazy 'spark transformations' are executed on each workernode and a value is returned to your driver program. This is the driver program.
        primairealarmenDemo1.cache();
        primairealarmenDemo1.show();
//        primairealarmenDemo1.printSchema();

        // schrijf alarm observaties weg gepartitioneerd op datum
        primairealarmenDemo1.write().mode("overwrite").partitionBy("datum").parquet("/Users/pieter/work/git/gezever/sosa-procedure/java-examples/output/parquet/primairealarmen/demo1");
        primairealarmenDemo1.write().mode("overwrite").partitionBy("datum").json("/Users/pieter/work/git/gezever/sosa-procedure/java-examples/output/json/primairealarmen/demo1");

        // Bereken en creeer primaire alarm observaties op basis van SQL query demo 2
        String selectPrimaireAlarmObservatiesDemo2 = """
                SELECT \
                   concat('observatie:', uuid()) as _id, \
                   'sosa:Observation' as _type, \
                   obs.hasFeatureOfInterest, \
                   obs.observedProperty, \
                   current_timestamp() as resultTime, \
                   obs.resultTime as phenomenonTime,
                   obs._id as wasInformedBy,
                   named_struct( "_id", concat('result:',uuid()),
                                 "_type", 'sosa:Result',
                                 "value", absnorm.bbid,
                                 "label", absnorm.type,
                                 "parametervalue", obs.result.numerieke_waarde,
                                 "parameterunit", obs.result.eenheid,
                                 "scopenote", absnorm.scopeNote
                               ) as result,
                   date(obs.resultTime) as datum
                FROM  observaties obs \
                JOIN  absolutenormbandbreedtes absnorm ON absnorm.lzsid = obs.hasFeatureOfInterest AND absnorm.parameter = obs.observedProperty \
                WHERE (absnorm.minValue is NULL OR obs.result.numerieke_waarde > absnorm.minValue) AND (absnorm.maxValue IS NULL OR obs.result.numerieke_waarde < absnorm.maxValue)""";

        Dataset<Row> primairealarmenDemo2 = spark.sql(selectPrimaireAlarmObservatiesDemo2);

        // cache or persist, multiple 'spark actions' on the same rdd.
        // Each time you peform a 'spark action' your lazy 'spark transformations' are executed on each workernode and a value is returned to your driver program. This is the driver program.
        primairealarmenDemo2.cache();
        primairealarmenDemo2.show();
//        primairealarmenDemo2.printSchema();


        // schrijf alarm observaties weg gepartitioneerd op datum
        primairealarmenDemo2.write().mode("overwrite").partitionBy("datum").parquet("/Users/pieter/work/git/gezever/sosa-procedure/java-examples/output/parquet/primairealarmen/demo2");
        primairealarmenDemo2.write().mode("overwrite").partitionBy("datum").json("/Users/pieter/work/git/gezever/sosa-procedure/java-examples/output/json/primairealarmen/demo2");

        // End spark session
        spark.stop();
    }

    private static void generatePrimaireAlarmenWithDataframeApi() {
        SparkSession spark = SparkSession.builder().appName("QueryAlarmen").master("local").getOrCreate();

        // Read json into dataframe
        Dataset<Row> lzs_df = spark.read().option("multiline", "true").json("java-examples/src/main/resources/datalake/lzs.json");
        Dataset<Row> bandbreedtes_df = spark.read().option("multiline", "true").json("java-examples/src/main/resources/datalake/normbandbreedtes.json");

        // Selecteer enkel info die we nodig hebben
        Dataset<Row> lzs = lzs_df.select(col("_id"), col("type"));
        Dataset<Row> operatingrange = lzs_df.select(col("_id"), explode(col("hasOperatingRange.hasOperatingProperty")).alias("operatingproperty"));

        // Join dataset en bereken absolute bandbreedtes
        Dataset<Row> joinedDataset = lzs.join(bandbreedtes_df, bandbreedtes_df.col("subject").equalTo(lzs.col("type")))
                .join(operatingrange, operatingrange.col("_id").equalTo(lzs.col("_id")).and(operatingrange.col("operatingproperty.forProperty").equalTo(bandbreedtes_df.col("forProperty"))), "left");

        // Bereken absolute bandbreedte en selecteer info die we nodig hebben om primaire alarm observaties te genereren
        Dataset<Row> absolutenormbandbreedtes = joinedDataset
                .withColumn("absMinValue",
                        when(col("operatie").equalTo("som"), coalesce(col("operatingproperty.maxValue").$plus(col("minValue")), col("operatingproperty.value").$plus(col("minValue"))))
                                .when(col("operatie").equalTo("product"), coalesce(col("operatingproperty.maxValue").multiply(col("minValue")), col("operatingproperty.value").multiply(col("minValue"))))
                                .otherwise(col("minValue")))
                .withColumn("absMaxValue",
                        when(col("operatie").equalTo("som"), coalesce(col("operatingproperty.minValue").$plus(col("maxValue")), col("operatingproperty.value").$plus(col("maxValue"))))
                                .when(col("operatie").equalTo("product"), coalesce(col("operatingproperty.minValue").multiply(col("maxValue")), col("operatingproperty.value").multiply(col("maxValue"))))
                                .otherwise(col("maxValue")))
                .select(lzs.col("_id").alias("lzsid"),
                        col("id").alias("bbid"),
                        col("forProperty").alias("parameter"),
                        coalesce(col("operatingproperty.value"), col("operatingproperty.minValue"), col("operatingproperty.maxValue")).alias("referentiewaarde"),
                        col("minValue").alias("relMinValue"),
                        col("maxValue").alias("relMaxValue"),
                        col("operatie"),
                        col("prefLabel").alias("label"),
                        col("dc_type").alias("type"),
                        col("scopeNote"),
                        col("absMinValue"),
                        col("absMaxValue"));

        // cache or persist, multiple 'spark actions' on the same rdd.
        // Each time you peform a 'spark action' your lazy 'spark transformations' are executed on each workernode and a value is returned to your driver program. This is the driver program.
//        absolutenormbandbreedtes.show();
//        absolutenormbandbreedtes.printSchema();

        // Selecteer abnormale parameter observaties en norm bandbreedte waarbinnen deze vallen
        Dataset<Row> observaties = spark.read().option("multiline", "true").json("java-examples/src/main/resources/datalake/observaties.json");
        Dataset<Row> observatiesInBandbreedte =
                observaties.join(absolutenormbandbreedtes, col("lzsid").equalTo(col("hasFeatureOfInterest")).and(col("parameter").equalTo(col("observedProperty"))))
                        .where(col("absMinValue").isNull().or(col("result.numerieke_waarde").gt(col("absMinValue")))
                                .and(col("absMaxValue").isNull().or(col("result.numerieke_waarde").lt(col("absMaxValue")))));

//        observatiesInBandbreedte.show();

        // Creeer primaire alarm observaties
        Dataset<Row> primairealarmobservaties = observatiesInBandbreedte.select(
                concat(lit("observation:"), uuid()).alias("_id"),
                lit("sosa:Observation").alias("_type"),
                col("hasFeatureOfInterest"),
                col("observedProperty"),
                current_timestamp().alias("resultTime"),
                col("resultTime").alias("phenomenonTime"),
                col("_id").alias("wasInformedBy"),
                struct(
                        concat(lit("observation:"), uuid()).alias("_id"),
                        lit("sosa:Result").alias("_type"),
                        col("bbid").alias("value"),
                        col("type").alias("label"),
                        col("result.numerieke_waarde").alias("parametervalue"),
                        col("result.eenheid").alias("parameterunit"),
                        col("scopeNote")
                ),
                to_date(col("resultTime")).alias("datum")
        );

        // cache or persist, multiple 'spark actions' on the same rdd.
        // Each time you peform a 'spark action' your lazy 'spark transformations' are executed on each workernode and a value is returned to your driver program. This is the driver program.
        primairealarmobservaties.cache();
        primairealarmobservaties.show();

        primairealarmobservaties.write().mode("overwrite").partitionBy("datum").parquet("/Users/pieter/work/git/gezever/sosa-procedure/java-examples/output/parquet/primairealarmen/demo3");
        primairealarmobservaties.write().mode("overwrite").partitionBy("datum").json("/Users/pieter/work/git/gezever/sosa-procedure/java-examples/output/json/primairealarmen/demo3");

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
