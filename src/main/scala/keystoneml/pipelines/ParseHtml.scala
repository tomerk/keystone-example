package keystoneml.pipelines

import java.io._
import java.net.URL
import java.util.Random
import java.util.regex.Pattern

import breeze.linalg._
import keystoneml.loaders.{CommonCrawlLoader, FlickrLoader}
import keystoneml.nodes.images.Convolver
import keystoneml.nodes.{FFTConvolver, LoopConvolver}
import keystoneml.utils.{Image, ImageUtils}
import keystoneml.workflow.{Identity, Pipeline}
import org.apache.commons.io.IOUtils
import org.apache.spark.bandit.{Action, BanditTrait}
import org.apache.spark.bandit.policies._
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.jsoup.Jsoup
import org.w3c.tidy.Tidy
import scopt.OptionParser

import scala.collection.JavaConverters._


/**
 * TODO: When writing the paper note that oftentimes it is bad to use regex for HTML and it can be prone to
 * vulnerabilities, but link to discussions about how it can still be okay
 *
 * followup of how that went wrong:
 * https://blog.codinghorror.com/protecting-your-cookies-httponly/
 * http://www.25hoursaday.com/weblog/2008/08/31/DevelopersUsingLibrariesIsNotASignOfWeakness.aspx
 *
 * Also challenges of parsing html with regex:
 * http://stackoverflow.com/questions/701166/can-you-provide-some-examples-of-why-it-is-hard-to-parse-xml-and-html-with-a-reg
 */
object ParseHtml extends Serializable with Logging {

  val appName = "ParseHtml"

  def run(sc: SparkContext, conf: PipelineConfig): Pipeline[Image, Image] = {
    //Set up some constants.
    // TODO: Only case where regex would be slower than pre-parsing is if we do something that requires multiple (MANY!) passes over the document or completely absurdly complex regexes...
    // e.g. detect anchor links, and if they are found extract attributes from the relevant point?
    // But even then it's a stronger argument that regex is an approximate-parser...

    val commoncrawl = CommonCrawlLoader(sc, "/Users/tomerk11/Desktop/commoncrawl").sample(false,0.01, seed = 0).repartition(4).cache()
    val numDocs = commoncrawl.count()
    logInfo(s"loaded $numDocs docs")
    val html = commoncrawl.take(30)(16)//Jsoup.connect("https://www.reddit.com/r/news").userAgent("jsoupbot/1.0").timeout(0).get().html()
    Jsoup.parse(commoncrawl.first())

    val tag = "a"//"[^/][^>\\s]*"
    val attr = "href"

    // Note we can't reliably match what is inside the tag like below because regex can't capture context-free html grammars
    // http://regexr.com
    //val HTML_TAG_PATTERN: String = s"<$tag[^>]+$attr\\s*=\\s*()[^>]*>(.*)</$tag>"
    val SLOW_EMAIL_REGEX_PATTERN = "[a-zA-Z0-9]+(?:(\\.|_)[A-Za-z0-9!#$%&'*+/=?^`{|}~-]+)*@(?!([a-zA-Z0-9]*\\.[a-zA-Z0-9]*\\.[a-zA-Z0-9]*\\.))(?:[A-Za-z0-9](?:[a-zA-Z0-9-]*[A-Za-z0-9])?\\.)+[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?".r
    val URL_REGEX_PATTERN = "[(http(s)?):\\/\\/(www\\.)?a-zA-Z0-9@:%._\\+~#=]{2,256}\\.[a-z]{2,6}\\b([-a-zA-Z0-9@:%_\\+.~#?&//=]*)".r
    val HTML_TAG_PATTERN = (s"<($tag)\\s(?:[^>]+\\s)?$attr" + "\\s*=\\s*(\"[^\"]*\"|'[^']*')[^>]*>").r
    val HTML_TAG_NO_ATTR_PATTERN = s"<($tag)(\\s[^>]*)?>".r
    // The below is a really bad phone regex! Matches dates, numbers, etc.
    val PHONE_REGEX = "(?:\\+?(\\d{1,3}))?([-. (]*(\\d{3})[-. )]*)?((\\d{3})[-. ]*(\\d{2,4})(?:[-.x ]*(\\d+))?)".r

    PHONE_REGEX.findAllMatchIn(commoncrawl.first()).map(_.group(0)).toBuffer

    val start = System.currentTimeMillis()

    //val links = HTML_TAG_PATTERN.findAllMatchIn(html).map(_.group(0)).toBuffer
    val numMatches = commoncrawl.map { html =>
      val doc = Jsoup.parse(html)
      //doc.select(s"$tag[$attr]").iterator().asScala.map(_.attr(attr)).size

      val matches = PHONE_REGEX.findAllMatchIn(doc.text()).map(_.group(0)).toList
      matches.length
    }.sum()

    // JTidy DOM is outdated & from 2000... not great (errors a lot on commoncrawl data, executor dies
    /*val tidyDOM = new Tidy().parseDOM(IOUtils.toInputStream(html), null)
    val links = tidyDOM.getElementsByTagName("a")*/

    //val doc = Jsoup.parse(html)
    //val links = doc.select(s"$tag[$attr]").iterator().asScala.map(_.attr(attr)).toBuffer
    //val links = doc.select(s"h1").iterator().asScala.map(_.tag()).toBuffer

    val end = System.currentTimeMillis()

    val time = (end - start).toDouble
    logInfo(s"took $time ms")
    logInfo(s"found $numMatches matches")

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
