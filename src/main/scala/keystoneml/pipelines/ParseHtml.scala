package keystoneml.pipelines

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}
import java.net.URL
import java.util.Random
import java.util.regex.Pattern

import breeze.linalg._
import keystoneml.loaders.FlickrLoader
import keystoneml.nodes.images.Convolver
import keystoneml.nodes.{FFTConvolver, LoopConvolver}
import keystoneml.utils.{Image, ImageUtils}
import keystoneml.workflow.{Identity, Pipeline}
import org.apache.spark.bandit.{Action, BanditTrait}
import org.apache.spark.bandit.policies._
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.jsoup.Jsoup
import scopt.OptionParser

import scala.collection.JavaConverters._


/**
 * TODO: When writing the paper note that oftentimes it is bad to use regex for HTML and it can be prone to
 * vulnerabilities, but link to discussions about how it can still be okay
 *
 * followup of how that went wrong:
 * https://blog.codinghorror.com/protecting-your-cookies-httponly/
 * http://www.25hoursaday.com/weblog/2008/08/31/DevelopersUsingLibrariesIsNotASignOfWeakness.aspx
 */
object ParseHtml extends Serializable with Logging {

  val appName = "ParseHtml"

  def run(sc: SparkContext, conf: PipelineConfig): Pipeline[Image, Image] = {
    //Set up some constants.

    val html = Jsoup.connect("https://www.reddit.com/r/news").userAgent("jsoupbot/1.0").timeout(0).get().html()

    val tag = "a"
    val attr = "href"

    // Note we can't reliably match what is inside the tag like below because regex can't capture context-free html grammars
    //val HTML_TAG_PATTERN: String = s"<$tag[^>]+$attr\\s*=\\s*()[^>]*>(.*)</$tag>"
    val HTML_TAG_PATTERN = (s"<($tag)\\s(?:[^>]+\\s)?$attr" + "\\s*=\\s*(\"[^\"]*\"|'[^']*')[^>]*>").r
    val HTML_TAG_NO_ATTR_PATTERN = s"<($tag)(\\s[^>]*)?>".r
    val start = System.currentTimeMillis()

    val links = HTML_TAG_NO_ATTR_PATTERN.findAllMatchIn(html).map(_.group(0)).toBuffer
    //val links2 = (s"<span[^>]+id" + "\\s*=\\s*(\"[^\"]*\"|'[^']*')[^>]*>").r.findAllMatchIn(html).map(_.group(1)).toBuffer

    /*val doc = Jsoup.parse(html)
    val links = doc.select(s"$tag[$attr]").iterator().asScala.map(_.attr(attr)).toBuffer
    val links2 = doc.select(s"span[id]").iterator().asScala.map(_.attr("id")).toBuffer*/

    val end = System.currentTimeMillis()

    val time = (end - start).toDouble
    logInfo(time.toString)
    logInfo(links.length.toString)
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
