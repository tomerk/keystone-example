package keystoneml.pipelines

import java.io.StringReader

import com.fasterxml.jackson.databind.ObjectMapper
import keystoneml.utils.Image
import keystoneml.workflow.{Identity, Pipeline}
import org.apache.spark.{SparkConf, SparkContext}
import org.jsoup.Jsoup
import scopt.OptionParser


case class JsonPath(path: Seq[Either[String, Int]])
object JsonPath {
  def apply(name: Boolean, path: Any*): JsonPath = {
    val eitherPath: Seq[Either[String, Int]] = path.map {
      case index: Int => Right(index)
      case field: String => Left(field)
    }

    JsonPath(eitherPath)
  }
}

/**
 * TODO: When writing the paper note that oftentimes it is bad to use regex for HTML and it can be prone to
 * vulnerabilities, but link to discussions about how it can still be okay
 *
 * followup of how that went wrong:
 * https://blog.codinghorror.com/protecting-your-cookies-httponly/
 * http://www.25hoursaday.com/weblog/2008/08/31/DevelopersUsingLibrariesIsNotASignOfWeakness.aspx
 */
object ParseJson extends Serializable with Logging {
  lazy val gsonParser = new com.google.gson.JsonParser()
  lazy val jacksonMapper = new ObjectMapper

  def jsonpParse(data: String, fields: Seq[JsonPath]): Seq[Option[String]] = {
    val json = javax.json.Json.createReader(new StringReader(data)).read()
    fields.map(path => {
      var node: javax.json.JsonValue = json
      path.path.foreach {
        case Left(fieldName) =>
          if (node != null) {
            node = node.asInstanceOf[javax.json.JsonObject].get(fieldName)
          }
        case Right(index) =>
          if (node != null) {
            node = node.asInstanceOf[javax.json.JsonArray].get(index)
          }
      }
      if (node != null) {
        Some(node.toString)
      } else {
        None
      }
    })
  }

  def jacksonParse(data: String, fields: Seq[JsonPath]): Seq[Option[String]] = {
    val rootNode = jacksonMapper.readTree(data)
    fields.map(path => {
      var node = rootNode
      path.path.foreach {
        case Left(fieldName) => node = node.path(fieldName)
        case Right(index) => node = node.path(index)
      }
      if (node.isMissingNode) {
        None
      } else {
        Some(node.asText())
      }
    })
  }

  def gsonParse(data: String, fields: Seq[JsonPath]): Seq[Option[String]] = {
    val json = gsonParser.parse(data)
    fields.map(path => {
      var node = json
      path.path.foreach {
        case Left(fieldName) =>
          if (!node.isJsonNull) {
            node = node.getAsJsonObject.get(fieldName)
          }
        case Right(index) =>
          if (!node.isJsonNull) {
            node = node.getAsJsonArray.get(index)
          }
      }
      if (!node.isJsonNull) {
        Some(node.getAsString)
      } else {
        None
      }
    })
  }



  val appName = "ParseJson"

  def run(sc: SparkContext, conf: PipelineConfig): Pipeline[Image, Image] = {
    val data = sc.textFile(conf.trainLocation, 16).repartition(16).cache()
    data.count()

    val fields = Seq(JsonPath(true, "categories", 0))
    val out = data.map(json => jsonpParse(json, fields)).collect()
    //abs:href uses StringUtil.resolve(baseUri, attr(attributeKey));
    Identity[Image]().toPipeline
  }

  case class PipelineConfig(
      trainLocation: String = "",
      labelLocation: String = "",
      outputLocation: String = "",
      policy: String = "",
      communicationRate: String = "5s",
      disableMulticore: Boolean = false,
      warmup: Option[Int] = None,
      numParts: Int = 64)

  def parse(args: Array[String]): PipelineConfig = new OptionParser[PipelineConfig](appName) {
    head(appName, "0.1")
    help("help") text("prints this usage text")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("outputLocation") required() action { (x,c) => c.copy(outputLocation=x) }
    opt[String]("labelLocation") required() action { (x,c) => c.copy(labelLocation=x) }
    opt[String]("policy") required() action { (x,c) => c.copy(policy=x) }
    opt[String]("communicationRate") action { (x,c) => c.copy(communicationRate=x) }
    opt[Unit]("disableMulticore") action { (x,c) => c.copy(disableMulticore=true) }
    opt[Int]("warmup") action { (x,c) => c.copy(warmup=Some(x)) }
    opt[Int]("numParts") action { (x,c) => c.copy(numParts=x) }
  }.parse(args, PipelineConfig()).get

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   *
   * @param args
   */
  def main(args: Array[String]) = {
    val appConfig = parse(args)

    val conf = new SparkConf().setAppName(s"$appName-${appConfig.policy}-${appConfig.communicationRate}-${appConfig.disableMulticore}").set(
      "spark.bandits.communicationRate",
      appConfig.communicationRate)
    conf.setIfMissing("spark.master", "local[4]")
    val sc = new SparkContext(conf)
    run(sc, appConfig)

    sc.stop()
  }
}
