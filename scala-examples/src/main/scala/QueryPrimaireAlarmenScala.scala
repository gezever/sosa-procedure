import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, explode, when}

/**
 *
 */
class QueryPrimaireAlarmenScala {
  def main(args: Array[String]): Unit = {
    generatePrimaireAlarmenBySqlQuery()
  }

  /**
   * Bereken primaire alarmen op basis van sql query.
   * De query die de absolute bandbreedtes per lzs berekent kan telkens on the fly gebeuren OF vooraf reeds gematerialiseerd zijn in bestaande persistente view of tabel.
   */
  private def generatePrimaireAlarmenBySqlQuery() = {
    val spark = SparkSession.builder.appName("QueryAlarmen").master("local").getOrCreate

    // Read json into dataframe// Read json into dataframe
    val lzs_df = spark.read.option("multiline", "true").json("java-examples/src/main/resources/datalake/lzs.json")
    val normen_df = spark.read.option("multiline", "true").json("java-examples/src/main/resources/datalake/normen.json")

    // Parquet or json files can also be used to create a temporary view and then used in SQL statements// Parquet or json files can also be used to create a temporary view and then used in SQL statements
    lzs_df.createOrReplaceTempView("lzsview")
    normen_df.createOrReplaceTempView("normview")

    // Bereken absolute 'norm' bandbreedtes per parameter per lzs// Bereken absolute 'norm' bandbreedtes per parameter per lzs
    // Een norm specifieert 'abnormale' bandbreedtes (outofboundranges) voor een parameter.// Een norm specifieert 'abnormale' bandbreedtes (outofboundranges) voor een parameter.
    // Deze bandbreedtes kunnen relatief t.o.v referentiewaarde uitgedrukt zijn (operatie = 'som' of 'product') of absoluut// Deze bandbreedtes kunnen relatief t.o.v referentiewaarde uitgedrukt zijn (operatie = 'som' of 'product') of absoluut
    val selectBandbreedtes ="""
      WITH lzs AS (SELECT _id, type FROM lzsview), \
      normaloperatingrange AS (SELECT _id as lzsid, explode(parameterconditie) as parameterrange FROM lzsview), \
      norm AS (SELECT klasse, parameter, explode(bandbreedtes) as bandbreedte FROM normview) \
      SELECT lzs._id as lzsid, \
             norm.parameter, \
             COALESCE(normaloperatingrange.parameterrange.value, normaloperatingrange.parameterrange.minValue, normaloperatingrange.parameterrange.maxValue) as referentiewaarde, \
             norm.bandbreedte.minValue as relatieveMinValueNormBandbreedte, \
             norm.bandbreedte.maxValue as relatieveMaxValueNormBandbreedte, \
             norm.bandbreedte.operatie as operatie, \
             norm.bandbreedte.label, \
             norm.bandbreedte.type, \
             norm.bandbreedte.note, \
             CASE WHEN norm.bandbreedte.operatie = 'som' THEN COALESCE(normaloperatingrange.parameterrange.maxValue + norm.bandbreedte.minValue, normaloperatingrange.parameterrange.value + norm.bandbreedte.minValue) \
                  WHEN norm.bandbreedte.operatie = 'product' THEN COALESCE(normaloperatingrange.parameterrange.maxValue * norm.bandbreedte.minValue, normaloperatingrange.parameterrange.value * norm.bandbreedte.minValue) \
                  WHEN norm.bandbreedte.operatie is NULL THEN norm.bandbreedte.minValue \
             END as minValue, \
             CASE WHEN norm.bandbreedte.operatie = 'som' THEN COALESCE(normaloperatingrange.parameterrange.minValue + norm.bandbreedte.maxValue, normaloperatingrange.parameterrange.value + norm.bandbreedte.maxValue) \
                  WHEN norm.bandbreedte.operatie = 'product' THEN COALESCE(normaloperatingrange.parameterrange.minValue * norm.bandbreedte.maxValue, normaloperatingrange.parameterrange.value * norm.bandbreedte.maxValue) \
                  WHEN norm.bandbreedte.operatie is NULL THEN norm.bandbreedte.maxValue \
             END as maxValue \
      FROM lzs \
      JOIN norm ON lzs.type = norm.klasse \
      LEFT JOIN normaloperatingrange ON normaloperatingrange.lzsid = lzs._id AND normaloperatingrange.parameterrange.parameter = norm.parameter"""

    val absnormbandbreedtes = spark.sql(selectBandbreedtes)

    absnormbandbreedtes.show()
    absnormbandbreedtes.printSchema()

    absnormbandbreedtes.createOrReplaceTempView("absnormbandbreedtes")
    val observaties = spark.read.option("multiline", "true").json("java-examples/src/main/resources/datalake/observaties.json")
    observaties.createOrReplaceTempView("observaties")

    val selectAlarmen ="""
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
      WHERE (absnorm.minValue is NULL OR obs.result.numerieke_waarde > absnorm.minValue) AND (absnorm.maxValue IS NULL OR obs.result.numerieke_waarde < absnorm.maxValue)"""

    val primairealarmen = spark.sql(selectAlarmen)


    primairealarmen.show()
    //        primairealarmen.write().mode("overwrite").parquet("/Users/pieter/work/git/gezever/sosa-procedure/java-examples/target/primairealarmen.parquet");//        primairealarmen.write().mode("overwrite").parquet("/Users/pieter/work/git/gezever/sosa-procedure/java-examples/target/primairealarmen.parquet");
    //        primairealarmen.write().mode("overwrite").json("/Users/pieter/work/git/gezever/sosa-procedure/java-examples/target/primairealarmen.json");//        primairealarmen.write().mode("overwrite").json("/Users/pieter/work/git/gezever/sosa-procedure/java-examples/target/primairealarmen.json");

    spark.stop()
  }

}
