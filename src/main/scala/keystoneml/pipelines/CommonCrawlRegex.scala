package keystoneml.pipelines

import java.io._

import keystoneml.loaders.CommonCrawlLoader
import keystoneml.utils.Image
import keystoneml.workflow.{Identity, Pipeline}
import net.greypanther.javaadvent.regex.Regex
import net.greypanther.javaadvent.regex.factories._
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.jsoup.Jsoup
import scopt.OptionParser

import scala.collection.JavaConverters._

case class RegexTask(id: String, doc: String, regexOptions: Seq[Regex]) {
  def getMatches(arm: Int): Array[String] = regexOptions(arm).getMatches(doc, Array(0)).asScala.map(_.head).toArray
}

/**
 * Extract regex from common crawl files
 */
object CommonCrawlRegex extends Serializable with Logging {

  val appName = "CommonCrawlRegex"

  def run(sc: SparkContext, conf: PipelineConfig): Pipeline[Image, Image] = {
    //Set up some constants.

    val commoncrawl = CommonCrawlLoader(sc, conf.trainLocation).repartitionAndSortWithinPartitions(
      new Partitioner {
      override def numPartitions = conf.numParts
      override def getPartition(key: Any) = {
        val id = key.asInstanceOf[String]
        id.hashCode % conf.numParts
      }
    }).cache()

    val numDocs = commoncrawl.count()
    logInfo(s"loaded $numDocs docs")

    //val regexp = "([A-Za-z]+)" // Match all words
    val tag = "a"
    val attr = "href"
    val SLOW_EMAIL_REGEX_PATTERN = "[a-zA-Z0-9]+(?:(\\.|_)[A-Za-z0-9!#$%&'*+/=?^`{|}~-]+)*@(?!([a-zA-Z0-9]*\\.[a-zA-Z0-9]*\\.[a-zA-Z0-9]*\\.))(?:[A-Za-z0-9](?:[a-zA-Z0-9-]*[A-Za-z0-9])?\\.)+[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?"
    val URL_REGEX_PATTERN = "[(http(s)?):\\/\\/(www\\.)?a-zA-Z0-9@:%._\\+~#=]{2,256}\\.[a-z]{2,6}\\b([-a-zA-Z0-9@:%_\\+.~#?&//=]*)"//"[(http(s)?):\\/\\/(www\\.)?a-zA-Z0-9@:%._\\+~#=]{2,256}\\.[a-z]{2,6}\\b([-a-zA-Z0-9@:%_\\+.~#?&//=]*)"
    val HTML_TAG_PATTERN = (s"<($tag)\\s(?:[^>]+\\s)?$attr" + "\\s*=\\s*(\"[^\"]*\"|'[^']*')[^>]*>")
    val HTML_TAG_NO_ATTR_PATTERN = s"<($tag)(\\s[^>]*)?>"
    // The below is a really bad phone regex! Matches dates, numbers, etc.
    val PHONE_REGEX = "(?:\\+?(\\d{1,3}))?([-. (]*(\\d{3})[-. )]*)?((\\d{3})[-. ]*(\\d{2,4})(?:[-.x ]*(\\d+))?)"
    //val regexp = "([A-Za-z]+\\s+[A-Za-z]+)" // Match some bigrams
    //val regexp = "([A-Za-z]+%)" // Match all words
    //val regexp = "([A-Za-z]+ed)[ \t\n\r]+([A-Za-z]+[A-Da-dF-Zf-z][A-Ca-cE-Ze-z][ \t\n\r]+)*(John|Alice|Jane|James|Walter|Lord|George|Jackal|returned|angel|ornament|ripped|riposte)[ \t\n\r]+"
    // This is an insane stack-overflowing regex (that doesn't work right with DKbrics anyway) for email I found online: http://emailregex.com
    //val regexp = "(?:[a-z0-9!#$%&'*+/=?^_`~-]+(?:\\.[a-z0-9!#$%&'*+/=?^_`~-]+)*|\"(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21\\x23-\\x5b\\x5d-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])*\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21-\\x5a\\x53-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])+)\\])"
    val regexp = URL_REGEX_PATTERN
    // TODO WARNME: REGEXES may not be threadsafe
    val factories = Seq[(String, Unit=>RegexFactory)](
      ("DkBricsAutomatonRegexFactory", _ => new DkBricsAutomatonRegexFactory),
      ("JRegexFactory", _ => new JRegexFactory),
      ("OroRegexFactory", _ => new OroRegexFactory),
      ("JavaUtilPatternRegexFactory", _ => new JavaUtilPatternRegexFactory)
      //("ComBasistechTclRegexFactory", _ => new ComBasistechTclRegexFactory)
    )

    val doc = commoncrawl.first()
    factories.foreach { case (libName, factory) =>
      val startedTime = System.currentTimeMillis()

      val matcher = factory().create(regexp)
      val matches = matcher.getMatches(doc._2, Array(0))

      val endTime = System.currentTimeMillis()
      logInfo(s"Finished $libName in ${endTime - startedTime} ms")

    }

    val start = System.currentTimeMillis()
    factories.foreach { case (libName, factory) =>
      logInfo(s"Starting $libName")
      val start = System.currentTimeMillis()

      val numMatches = commoncrawl.mapPartitions(it => {
        val matcher = factory().create(regexp)
        it.map { doc =>
          val text = doc._2//Jsoup.parse(doc).text()
          matcher.getMatches(text, Array(0)).asScala.size
        }
      }).sum()

      val endTime = System.currentTimeMillis()
      logInfo(s"Finished $libName in ${endTime - start} ms")
      logInfo(s"found $numMatches matches")
    }

    val end = System.currentTimeMillis()

    val time = (end - start).toDouble

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
