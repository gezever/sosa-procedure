import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{SparkSession, DataFrame}
import com.github.jsonldjava.core.{JsonLdOptions, JsonLdProcessor}
import com.github.jsonldjava.utils.JsonUtils
import org.apache.jena.query._
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFParser}
import org.apache.jena.shacl.{ShaclValidator, Shapes}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule



import java.io.{ByteArrayOutputStream, FileOutputStream, FileWriter}
import scala.collection.JavaConverters._

import java.io.FileInputStream

object AlarmValidator {
  def main(args: Array[String]): Unit = {
    // 1. Bestanden
    val SHAPES = "src/main/resources/be/vlaanderen/omgeving/lzs/alarmValidator/shacl.ttl"
    val DATA = "src/main/resources/be/vlaanderen/omgeving/lzs/alarmValidator/input.jsonld"

    val frameString = scala.io.Source.fromFile("src/main/resources/be/vlaanderen/omgeving/lzs/alarmValidator/frame.json", "utf-8").getLines.mkString
    val constructAlarmFromValidationResult = scala.io.Source.fromFile("src/main/resources/be/vlaanderen/omgeving/lzs/alarmValidator/constructAlarmFromValidationResult.rq", "utf-8").getLines.mkString

    val ALARM_JSONLD = "output/alarmValidator/alarm_output.jsonld"
    val ALARM_TTL = "output/alarmValidator/alarm_output.ttl"

    // 2. Validatie jsonld tov. shacl => resultaat = report
    val shapesGraph = RDFDataMgr.loadGraph(SHAPES)
    val dataGraph = RDFDataMgr.loadGraph(DATA)
    val shapes = Shapes.parse(shapesGraph)
    val report = ShaclValidator.get.validate(shapes, dataGraph)
    //System.out.println()

    // 3. Construct alarm observatie
    // 3.1. Combineer Data en report in 1 Model
    val observatieModel = ModelFactory.createDefaultModel()
    RDFParser.create()
      .source(new FileInputStream(DATA))
      .lang(Lang.JSONLD)
      .parse(observatieModel)
    val combinedModel = ModelFactory.createUnion(observatieModel, report.getModel)
    //combinedModel.write(System.out, "TURTLE")

    // 3.2. Compileer de query
    val query: Query = QueryFactory.create(constructAlarmFromValidationResult)
    // 3.3. Voer de CONSTRUCT uit op het rapportmodel
    val qexec: QueryExecution = QueryExecutionFactory.create(query, combinedModel)
    val resultModel: Model = try {
      qexec.execConstruct()
    } finally {
      qexec.close()
    }

    // 3.4. Serialiseer het query-resultaat naar een output TURTLE bestand
    resultModel.write(System.out, "TURTLE")
    val output = new FileOutputStream(ALARM_TTL)
    try {
      resultModel.write(output, "TURTLE") // Other formats: "RDF/XML", "N-TRIPLES", "TURTLE"
    } finally {
      output.close()
    }

    // 3.5. Serialiseer het query-resultaat naar JSONLD en FRAME dit
    val out = new ByteArrayOutputStream()
    resultModel.write(out, "JSON-LD")
    val jsonldString = out.toString("UTF-8")
    val jsonObject: Object = JsonUtils.fromString(jsonldString)
    val frame: Object = JsonUtils.fromString(frameString)
    val options: JsonLdOptions = new JsonLdOptions
    val framed: java.util.Map[String, Object] = JsonLdProcessor.frame(jsonObject, frame, options)

    // 3.6 Schijf geframed jsonld naar een bestand
    val outputFile = new FileWriter(ALARM_JSONLD)
    try {
      JsonUtils.writePrettyPrint(outputFile, framed) // or JsonUtils.write(outputFile, framed)
    } finally {
      outputFile.close()
    }

    // 4. Parse @graph value uit jsonld, lees dit in een spark dataframe en schrijf dit weg naar parquet
    val spark = SparkSession.builder()
      .appName("JsonLDToParquet")
      .master("local[*]")
      .getOrCreate()
    // 4.1. parse @graph
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val rootNode: JsonNode = mapper.readTree(JsonUtils.toPrettyString(framed))
    val graphArray: JsonNode = rootNode.get("@graph")

    // 4.2. Converteer @graph-array naar een scala Map voor Spark
    val records = graphArray.elements().asScala.map { node =>
      mapper.convertValue[Map[String, Any]](node, classOf[Map[String, Any]])
    }.toSeq

    // 4.3. Lees in Spark DataFrame
    import spark.implicits._
    val df: DataFrame = spark.read.json(spark.createDataset(records.map(mapper.writeValueAsString)))

    // 4.4. Toon (optioneel)
    df.printSchema()
    df.show(truncate = false)

    // 4.5. Schrijf als Parquet
    df.coalesce(1).write.mode("overwrite").parquet("output/alarmValidator/alarmValidator_parquet")
    //Spark schrijft de Parquet-bestanden per partitie ; onderstaande lijn maakt 2 parquetbestanden
    //df.write.mode("overwrite").parquet("output/graph-data.parquet")
    //
    //JsonUtils.toPrettyString(framed)

  }

}