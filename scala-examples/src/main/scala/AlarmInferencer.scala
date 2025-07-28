import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.RDFParser
import org.apache.jena.riot.Lang
import org.apache.jena.reasoner.ReasonerRegistry
import java.io.FileInputStream
//import openllet.jena.PelletReasonerFactory

object AlarmInferencer {
  def main(args: Array[String]): Unit = {
    val observatieModel = ModelFactory.createDefaultModel()
    val observatie_input = new FileInputStream("scala-examples/src/main/resources/input.jsonld")

    RDFParser.create()
      .source(observatie_input)
      .lang(Lang.JSONLD)
      .parse(observatieModel)

    val ontologyModel = ModelFactory.createDefaultModel()
    val alarm_input = new FileInputStream("scala-examples/src/main/resources/alarmen.ttl")

    RDFParser.create()
      .source(alarm_input)
      .lang(Lang.TURTLE)
      .parse(ontologyModel)

    val combinedModel = ontologyModel.union(observatieModel)

    //val pelletReasoner = PelletReasonerFactory.theInstance().create()
    //val infModel = ModelFactory.createInfModel(pelletReasoner, combinedModel)
    //val combinedModel = ModelFactory.createUnion(ontologyModel, observatieModel)
    // Attach OWL reasoner
    val reasoner = ReasonerRegistry.getOWLReasoner.bindSchema(ontologyModel)
    //reasoner.setDerivationLogging(true)
    //val reasoner2 = ReasonerRegistry.getOWLReasoner();

    val infModel = ModelFactory.createInfModel(reasoner, combinedModel)
    // Optional: print the model to stdout
    observatieModel.write(System.out, "TURTLE")
    //observatieModel.write(System.out, "TURTLE")  // or "RDF/XML", "N-TRIPLES", etc.
  }
}