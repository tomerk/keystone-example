package keystoneml.pipelines

import java.io.StringReader

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.JsonStreamParser
import keystoneml.utils.Image
import keystoneml.workflow.{Identity, Pipeline}
import org.apache.spark.{SparkConf, SparkContext}
import org.jsoup.Jsoup
import scopt.OptionParser


case class JsonPath(path: Seq[Either[String, Int]])
object JsonPath {
  def at(path: Any*): JsonPath = {
    val eitherPath: Seq[Either[String, Int]] = path.map {
      case index: Int => Right(index)
      case field: String => Left(field)
    }

    JsonPath(eitherPath)
  }
}

/**
* json libraries sourced from http://blog.takipi.com/the-ultimate-json-library-json-simple-vs-gson-vs-jackson-vs-json/
 */
object ParseJson extends Serializable with Logging {
  lazy val gsonParser = new com.google.gson.JsonParser()
  lazy val jacksonMapper = new ObjectMapper

  def jsonSimpleParse(data: String, fields: Seq[JsonPath]): Seq[Option[String]] = {
    // The json simple parser is not even close to thread-safe!
    val json = org.json.simple.JSONValue.parse(data)

    fields.map(path => try {
      var node: Any = json
      path.path.foreach {
        case Left(fieldName) =>
          if (node != null) {
            node = node.asInstanceOf[org.json.simple.JSONObject].get(fieldName)
          }
        case Right(index) =>
          if (node != null) {
            node = node.asInstanceOf[org.json.simple.JSONArray].get(index)
          }
      }
      if (node != null) {
        Some(node.toString)
      } else {
        None
      }
    } catch {
      case _: IndexOutOfBoundsException => None
    })
  }

  def jsonpParse(data: String, fields: Seq[JsonPath]): Seq[Option[String]] = {
    val parser = javax.json.Json.createReader(new StringReader(data))
    val json = parser.read()
    parser.close()

    fields.map(path => try {
      var node: javax.json.JsonValue = json
      path.path.foreach {
        case Left(fieldName) =>
          if (node != javax.json.JsonValue.NULL) {
            node = node.asInstanceOf[javax.json.JsonObject].get(fieldName)
          }
        case Right(index) =>
          if (node != javax.json.JsonValue.NULL) {
            node = node.asInstanceOf[javax.json.JsonArray].get(index)
          }
      }
      if (node != javax.json.JsonValue.NULL) {
        Some(node.toString)
      } else {
        None
      }
    } catch {
      case _: IndexOutOfBoundsException => None
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
    fields.map(path => try {
      var node = json
      path.path.foreach {
        case Left(fieldName) =>
          if (node != null && !node.isJsonNull) {
            node = node.getAsJsonObject.get(fieldName)
          }
        case Right(index) =>
          if (node != null && !node.isJsonNull) {
            node = node.getAsJsonArray.get(index)
          }
      }
      if (node != null && !node.isJsonNull) {
        Some(node.getAsString)
      } else {
        None
      }
    } catch {
      case _: IndexOutOfBoundsException => None
    })
  }

  val appName = "ParseJson"

  def run(sc: SparkContext, conf: PipelineConfig): Pipeline[Image, Image] = {
    val data = sc.wholeTextFiles(conf.trainLocation, 16).map(_._2).repartition(16).cache()
    val num = data.count()
    logInfo(s"$num")

    val fields = Seq(JsonPath.at("type"))
    val json = data.first()
    val start = System.nanoTime()
    //val out = data.map(json => jsonSimpleParse(json, fields)).collect()
    jacksonParse(json, fields)
    val end = System.nanoTime()
    logInfo(s"Finished. ${end - start}")
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
