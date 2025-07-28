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

import java.io.File


import java.io.{ByteArrayOutputStream, FileOutputStream, FileWriter}
import scala.collection.convert.ImplicitConversions.`map AsJavaMap`
//import org.topbraid.shacl.validation.ValidationReport
import scala.collection.JavaConverters._

import java.io.FileInputStream

object AlarmValidator {
  def main(args: Array[String]): Unit = {

    val SHAPES = "scala-examples/src/main/resources/shacl.ttl"
    val DATA = "scala-examples/src/main/resources/input.jsonld"
    val ALARM = "scala-examples/src/main/resources/alarm_output.jsonld"
    val ALARM_TTL = "scala-examples/src/main/resources/alarm_output.ttl"
    //val context = new ObjectMapper().readTree(new FileInputStream("scala-examples/src/main/resources/be/vlaanderen/omgeving/lzs/context.json"))
    val frameFile = "scala-examples/src/main/resources/input.json"
    val frameString = scala.io.Source.fromFile("scala-examples/src/main/resources/be/vlaanderen/omgeving/lzs/frame.json", "utf-8").getLines.mkString



    val shapesGraph = RDFDataMgr.loadGraph(SHAPES)
    val dataGraph = RDFDataMgr.loadGraph(DATA)

    val shapes = Shapes.parse(shapesGraph)

    val report = ShaclValidator.get.validate(shapes, dataGraph)
    //ShLib.printReport(report)
    System.out.println()
    //RDFDataMgr.write(System.out, report.getModel, Lang.TTL)

    val observatieModel = ModelFactory.createDefaultModel()


    RDFParser.create()
      .source(new FileInputStream(DATA))
      .lang(Lang.JSONLD)
      .parse(observatieModel)

    //val combinedModel: Model = observatieModel.union(report.getModel)
    val combinedModel = ModelFactory.createUnion(observatieModel, report.getModel)
    combinedModel.write(System.out, "TURTLE")

    val constructQueryStr =
      """
        PREFIX sh: <http://www.w3.org/ns/shacl#>
        PREFIX sosa: <http://www.w3.org/ns/sosa/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX observatie: <https://data.lzs.omgeving.vlaanderen.be/id/observation/>
        PREFIX prov: <http://www.w3.org/ns/prov#>
        CONSTRUCT {
          ?newObs a sosa:Observation, ?resultSeverity;
          prov:wasInformedBy  ?focusNode ;
          sosa:hasFeatureOfInterest ?hasFeatureOfInterest;
          sosa:observedProperty      ?observedProperty ;
          sosa:resultTime ?now ;
          sosa:phenomenonTime ?tijd;
          sosa:hasResult   [ rdf:type   sosa:Result;
                             rdf:value  ?message ];
           .
        }
        WHERE {
          ?v a sh:ValidationResult ;
             sh:focusNode ?focusNode ;
             sh:resultSeverity ?resultSeverity;
             sh:resultMessage ?message .
          ?focusNode sosa:hasFeatureOfInterest ?hasFeatureOfInterest;
              sosa:resultTime ?tijd ;
              sosa:observedProperty      ?observedProperty.

        BIND(STR(?focusNode) AS ?valStr)
        BIND(STR(?tijd) AS ?tijdStr)
        BIND(CONCAT(?valStr, "-", ?tijdStr) AS ?rawStr)
        BIND(MD5(?rawStr) AS ?hash)
        BIND(IRI(CONCAT("https://data.lzs.omgeving.vlaanderen.be/id/observation/", ?hash)) AS ?newObs)
        BIND(NOW() AS ?now)
               }
        """.stripMargin



    // Compileer de query
    val query: Query = QueryFactory.create(constructQueryStr)

    // Voer de CONSTRUCT uit op het rapportmodel
    val qexec: QueryExecution = QueryExecutionFactory.create(query, combinedModel)

    val resultModel: Model = try {
      qexec.execConstruct()
    } finally {
      qexec.close()
    }

    // Schrijf het resultaat naar stdout of sla het op
    resultModel.write(System.out, "TURTLE")

    val output = new FileOutputStream(ALARM_TTL)
    try {
      resultModel.write(output, "TURTLE") // Other formats: "RDF/XML", "N-TRIPLES", "TURTLE"
    } finally {
      output.close()
    }

    val out = new ByteArrayOutputStream()
    resultModel.write(out, "JSON-LD")
    val jsonldString = out.toString("UTF-8")
    val jsonObject: Object = JsonUtils.fromString(jsonldString)
    val frame: Object = JsonUtils.fromString(frameString)
    val options: JsonLdOptions = new JsonLdOptions
    val framed: java.util.Map[String, Object] = JsonLdProcessor.frame(jsonObject, frame, options)

    val outputFile = new FileWriter(ALARM)
    try {
      JsonUtils.writePrettyPrint(outputFile, framed) // or JsonUtils.write(outputFile, framed)
    } finally {
      outputFile.close()
    }

    val spark = SparkSession.builder()
      .appName("JsonLDToParquet")
      .master("local[*]")
      .getOrCreate()

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val rootNode: JsonNode = mapper.readTree(JsonUtils.toPrettyString(framed))
    val graphArray: JsonNode = rootNode.get("@graph")

    // 4. Converteer @graph-array naar een Seq[Map[String, Any]] voor Spark
    val records = graphArray.elements().asScala.map { node =>
      mapper.convertValue[Map[String, Any]](node, classOf[Map[String, Any]])
    }.toSeq

    // 5. Lees in Spark DataFrame
    import spark.implicits._
    val df: DataFrame = spark.read.json(graphArray.toString)

    //val df: DataFrame = spark.read.json(spark.createDataset(records.map(mapper.writeValueAsString)))

    // 6. Toon (optioneel)
    df.printSchema()
    df.show(truncate = false)

    // 7. Schrijf als Parquet
    df.write.mode("overwrite").parquet("output/graph-data.parquet")
    //JsonUtils.toPrettyString(framed)

  }

}