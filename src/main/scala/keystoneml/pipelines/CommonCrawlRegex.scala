package keystoneml.pipelines

import java.io._

import breeze.linalg.DenseVector
import keystoneml.bandits.{ConstantBandit, OracleBandit}
import keystoneml.loaders.CommonCrawlLoader
import net.greypanther.javaadvent.regex.Regex
import net.greypanther.javaadvent.regex.factories._
import org.apache.spark.bandit.BanditTrait
import org.apache.spark.bandit.policies._
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.jsoup.Jsoup
import scopt.OptionParser

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.hashing.MurmurHash3

case class RegexTask(id: String, doc: String) {
  //def getMatches(arm: Int): Array[String] = regexOptions(arm).getMatches(doc, Array(0)).asScala.map(_.head).toArray
}

abstract class RegexFeature extends Serializable {
  def get(task: RegexTask): Double
}

object RegexFactoryContainer extends Serializable {
  @transient lazy val factories: Seq[RegexFactory] = Seq(
    new ComBasistechTclRegexFactory,
    new JRegexFactory,
    new OroRegexFactory,
    new JavaUtilPatternRegexFactory
  )

  //  http://regexr.com
  val regexes: Seq[String] = Seq("[(http(s)?):\\/\\/(www\\.)?a-zA-Z0-9@:%._\\+~#=]{2,255}\\.[a-z]{2,6}",
    "([A-Za-z]+[ \\t\\n\\r]+[A-Za-z]+[ \\t\\n\\r]+[A-Za-z]+)",
    "<(a)[ \\t\\n\\r]([^>]+[ \\t\\n\\r])?href[ \\t\\n\\r]*=[ \\t\\n\\r]*(\"[^\"]*\"|'[^']*')[^>]*>",
    "(\\+?([0-9]{1,3}))?([-. (]*([0-9]{3})[-. )]*)?(([0-9]{3})[-. ]*([0-9]{2,4})([-.x ]*([0-9]+))?)",
    "[a-zA-Z0-9]+((\\.|_)[A-Za-z0-9!#$%&'*+/=?^`~-]+)*@(?!([a-zA-Z0-9]*\\.[a-zA-Z0-9]*\\.[a-zA-Z0-9]*\\.))([A-Za-z0-9]([a-zA-Z0-9-]*[A-Za-z0-9])?\\.)+[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?",
    "[a-z0-9!#$%&'*+/=?^_`~-]+(\\.[a-z0-9!#$%&'*+/=?^_`~-]+)*@([a-z0-9]([a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9]([a-z0-9-]*[a-z0-9])?",
    "\\$([1-9][0-9]{0,2}|[0-9])(\\,[0-9]{3})*(\\.[0-9]+)?(?=[ \\t\\n\\r]|$)", // Find dollar counts
    "#([a-fA-F\\d]{3}){1,2}|[rR][gG][bB]\\(((\\s*0*(25[0-5]|2[0-4]\\d|1?\\d?\\d)\\s*,){2}\\s*0*(25[0-5]|2[0-4]\\d|1?\\d?\\d)|\\s*0*(100(\\.0+)?|\\d?\\d(\\.\\d+)?)%(\\s*,\\s*0*(100(\\.0+)?|\\d?\\d(\\.\\d+)?)%){2})\\s*\\)|hsl\\(\\s*0*(360|3[0-5]\\d|[12]?\\d?\\d)\\s*(,\\s*0*(100(\\.0+)?|\\d?\\d(\\.\\d+)?)%\\s*){2}\\)|([rR][gG][bB][aA]\\(((\\s*0*(25[0-5]|2[0-4]\\d|1?\\d?\\d)\\s*,){3}|(\\s*0*(100(\\.0+)?|\\d?\\d(\\.\\d+)?)%\\s*,){3})|[hH][sS][lL][aA]\\(\\s*0*(360|3[0-5]\\d|[12]?\\d?\\d)\\s*(,\\s*0*(100(\\.0+)?|\\d?\\d(\\.\\d+)?)%\\s*){2},)\\s*0*(1|0(\\.\\d+)?)\\s*\\)", // CSS colors regexr: http://regexr.com/38lmo slightly edited to be case insensitive & not use word boundaries
    "(?:(?:2(?:[0-4][0-9]|5[0-5])|[0-1]?[0-9]?[0-9])\\.){3}(?:(?:2([0-4][0-9]|5[0-5])|[0-1]?[0-9]?[0-9]))"

  )
  /*Seq("[(http(s)?):\\/\\/(www\\.)?a-zA-Z0-9@:%._\\+~#=]{2,255}\\.[a-z]{2,6}\\b([-a-zA-Z0-9@:%_\\+.~#?&//=]*)", // Had to change {2,256} to {2,255} for combasistech
      "([A-Za-z]+[ \\t\\n\\r]+[A-Za-z]+[ \\t\\n\\r]+[A-Za-z]+)",
      "<(a)[ \\t\\n\\r]([^>]+[ \\t\\n\\r])?href[ \\t\\n\\r]*=[ \\t\\n\\r]*(\"[^\"]*\"|'[^']*')[^>]*>",
      "(\\+?([0-9]{1,3}))?([-. (]*([0-9]{3})[-. )]*)?(([0-9]{3})[-. ]*([0-9]{2,4})([-.x ]*([0-9]+))?)",
      "[a-zA-Z0-9]+((\\.|_)[A-Za-z0-9!#$%&'*+/=?^`~-]+)*@(?!([a-zA-Z0-9]*\\.[a-zA-Z0-9]*\\.[a-zA-Z0-9]*\\.))([A-Za-z0-9]([a-zA-Z0-9-]*[A-Za-z0-9])?\\.)+[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?")*/
}

case class RegexContainer(factory: Int, regexp: Int) {
  @transient lazy val regex: Regex = RegexFactoryContainer.factories(factory).create(RegexFactoryContainer.regexes(regexp))
}

case class RegexRecord(partitionId: Int,
                          posInPartition: Int,
                          canonicalTupleId: String,
                          docLength: Int,
                          reward: Double,
                          arm: Int)

case class RegexBias() extends RegexFeature {
  override def get(task: RegexTask): Double = 1.0
}
case class RegexDocLength() extends RegexFeature {
  override def get(task: RegexTask): Double = task.doc.length
}

/**
 * Extract regex from common crawl files
 */
object CommonCrawlRegex extends Serializable with Logging {
  def regexMatcher(factory: RegexContainer, task: RegexTask): mutable.Buffer[Array[String]] = {
    val matcher = factory.regex
    matcher.getMatches(task.doc, Array(0)).asScala.toBuffer
  }

  val appName = "CommonCrawlRegex"

  def makeFeatures(features: String): (RegexTask => DenseVector[Double], Int) = {
    val featureList: Array[RegexFeature] = features.split(',').map {
      case "bias" => RegexBias()
      case "doc_length" => RegexDocLength()
      case unknown => throw new IllegalArgumentException(s"Unknown feature $unknown")
    }

    (extractFeatures(featureList, _), featureList.length)
  }

  def extractFeatures(featureList: Array[RegexFeature], task: RegexTask): DenseVector[Double] = {
    DenseVector(featureList.map(_.get(task)))
  }

  def minOracle(path: String): RegexTask => Int = {
    val records = Source.fromFile(path).getLines.filter(x => !x.startsWith("partition_id")).map {
      line =>
        val splitLine = line.split(',')
        RegexRecord(
          partitionId = splitLine(0).toInt,
          posInPartition = splitLine(1).toInt,
          canonicalTupleId = splitLine(2),
          docLength = splitLine(3).toInt,
          reward = splitLine(7).toDouble,
          arm = splitLine(6).toInt)
    }.toSeq

    val armRewards =
    // Group by convolution id
      records.groupBy(_.canonicalTupleId)
        // For each convolution id, group + sum the rewards by arm
        .mapValues(_.groupBy(_.arm).mapValues(_.map(_.reward).sum))

    // we need a map identity here because a mapValues bug means this isn't serializable
    val bestArms = armRewards.mapValues(_.maxBy(_._2)._1).map(identity)

    val oracle: RegexTask => Int = task => bestArms(task.id)
    oracle
  }

  def run(sc: SparkContext, conf: PipelineConfig): Unit = {
    //Set up some constants.


    //val regexp = "([A-Za-z]+)" // Match all words
    val tag = "a"
    val attr = "href"
    val SLOW_EMAIL_REGEX_PATTERN = "[a-zA-Z0-9]+((\\.|_)[A-Za-z0-9!#$%&'*+/=?^`{|}~-]+)*@(?!([a-zA-Z0-9]*\\.[a-zA-Z0-9]*\\.[a-zA-Z0-9]*\\.))([A-Za-z0-9]([a-zA-Z0-9-]*[A-Za-z0-9])?\\.)+[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?"
    val URL_REGEX_PATTERN = "[(http(s)?):\\/\\/(www\\.)?a-zA-Z0-9@:%._\\+~#=]{2,256}\\.[a-z]{2,6}\\b([-a-zA-Z0-9@:%_\\+.~#?&//=]*)"//"[(http(s)?):\\/\\/(www\\.)?a-zA-Z0-9@:%._\\+~#=]{2,256}\\.[a-z]{2,6}\\b([-a-zA-Z0-9@:%_\\+.~#?&//=]*)"
    val HTML_TAG_PATTERN = (s"<($tag)\\s([^>]+\\s)?$attr" + "\\s*=\\s*(\"[^\"]*\"|'[^']*')[^>]*>")
    val HTML_TAG_NO_ATTR_PATTERN = s"<($tag)(\\s[^>]*)?>"
    // The below is a really bad phone regex! Matches dates, numbers, etc.
    val PHONE_REGEX = "(\\+?(\\d{1,3}))?([-. (]*(\\d{3})[-. )]*)?((\\d{3})[-. ]*(\\d{2,4})([-.x ]*(\\d+))?)"
    //val regexp = "([A-Za-z]+\\s+[A-Za-z]+)" // Match some bigrams
    //val regexp = "([A-Za-z]+%)" // Match all words
    //val regexp = "([A-Za-z]+ed)[ \t\n\r]+([A-Za-z]+[A-Da-dF-Zf-z][A-Ca-cE-Ze-z][ \t\n\r]+)*(John|Alice|Jane|James|Walter|Lord|George|Jackal|returned|angel|ornament|ripped|riposte)[ \t\n\r]+"
    // This is an insane stack-overflowing regex (that doesn't work right with DKbrics anyway) for email I found online: http://emailregex.com
    //val regexp = "([a-z0-9!#$%&'*+/=?^_`~-]+(\\.[a-z0-9!#$%&'*+/=?^_`~-]+)*|\"([\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21\\x23-\\x5b\\x5d-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])*\")@(([a-z0-9]([a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9]([a-z0-9-]*[a-z0-9])?|\\[((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:([\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21-\\x5a\\x53-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])+)\\])"
    val regexp = URL_REGEX_PATTERN
    // TODO WARNME: REGEXES may not be threadsafe
    val factories = Seq[(String, Unit=>RegexFactory)](
      //("DkBricsAutomatonRegexFactory", _ => new DkBricsAutomatonRegexFactory),
      ("JRegexFactory", _ => new JRegexFactory),
      ("OroRegexFactory", _ => new OroRegexFactory),
      ("JavaUtilPatternRegexFactory", _ => new JavaUtilPatternRegexFactory)
      //("ComBasistechTclRegexFactory", _ => new ComBasistechTclRegexFactory)
    )

    //val regexes = conf.regex.split("....")
    val regexOps: Seq[RegexTask => mutable.Buffer[Array[String]]] = RegexFactoryContainer.factories.indices.map(i => regexMatcher(RegexContainer(i, conf.regex), _: RegexTask))

    val bandit: BanditTrait[RegexTask, mutable.Buffer[Array[String]]] = conf.policy.trim.toLowerCase.split(':') match {
      // Constant Policies
      case Array("constant", arm) =>
        new ConstantBandit(arm.toInt, regexOps(arm.toInt))

      // Oracle Policies
      case Array("oracle", "min", path) =>
        new OracleBandit(minOracle(path), regexOps)

      // Non-contextual policies
      case Array("epsilon-greedy") =>
        sc.bandit(regexOps, EpsilonGreedyPolicyParams())
      case Array("epsilon-greedy", epsilon) =>
        sc.bandit(regexOps, EpsilonGreedyPolicyParams(epsilon.toDouble))
      case Array("gaussian-thompson-sampling") =>
        sc.bandit(regexOps, GaussianThompsonSamplingPolicyParams())
      case Array("gaussian-thompson-sampling", varMultiplier) =>
        sc.bandit(regexOps, GaussianThompsonSamplingPolicyParams(varMultiplier.toDouble))
      case Array("ucb1-normal") =>
        sc.bandit(regexOps, UCB1NormalPolicyParams())
      case Array("ucb1-normal", rewardRange) =>
        sc.bandit(regexOps, UCB1NormalPolicyParams(rewardRange.toDouble))
      case Array("ucb-gaussian-bayes") =>
        sc.bandit(regexOps, GaussianBayesUCBPolicyParams())
      case Array("ucb-gaussian-bayes", rewardRange) =>
        sc.bandit(regexOps, GaussianBayesUCBPolicyParams(rewardRange.toDouble))

      // Contextual policies
      case Array("contextual-epsilon-greedy", featureString) =>
        val (features, numFeatures) = makeFeatures(featureString)
        sc.contextualBandit(regexOps, features, ContextualEpsilonGreedyPolicyParams(numFeatures))
      case Array("contextual-epsilon-greedy", featureString, epsilon) =>
        val (features, numFeatures) = makeFeatures(featureString)
        sc.contextualBandit(regexOps, features, ContextualEpsilonGreedyPolicyParams(numFeatures, epsilon.toDouble))
      case Array("linear-thompson-sampling", featureString) =>
        val (features, numFeatures) = makeFeatures(featureString)
        sc.contextualBandit(regexOps, features, LinThompsonSamplingPolicyParams(numFeatures, 2.0))
      case Array("linear-thompson-sampling", featureString, useCholesky, varMultiplier) =>
        val (features, numFeatures) = makeFeatures(featureString)
        sc.contextualBandit(regexOps, features, LinThompsonSamplingPolicyParams(numFeatures, varMultiplier.toDouble, useCholesky = useCholesky.toBoolean))
      case Array("slinear-thompson-sampling", featureString) =>
        val (features, numFeatures) = makeFeatures(featureString)
        sc.contextualBandit(regexOps, features, new StandardizedLinThompsonSamplingPolicy(regexOps.length, numFeatures, 1.0, useCholesky = true))
      case Array("slinear-thompson-sampling", featureString, useCholesky, varMultiplier) =>
        val (features, numFeatures) = makeFeatures(featureString)
        sc.contextualBandit(regexOps, features, new StandardizedLinThompsonSamplingPolicy(regexOps.length, numFeatures, varMultiplier.toDouble, useCholesky = useCholesky.toBoolean))
      case Array("lin-ucb", featureString) =>
        val (features, numFeatures) = makeFeatures(featureString)
        sc.contextualBandit(regexOps, features, LinUCBPolicyParams(numFeatures))
      case Array("lin-ucb", featureString, alpha) =>
        val (features, numFeatures) = makeFeatures(featureString)
        sc.contextualBandit(regexOps, features, LinUCBPolicyParams(numFeatures, alpha.toDouble))

      case _ =>
        throw new IllegalArgumentException(s"Invalid policy ${conf.policy}")
    }

    val commoncrawl = CommonCrawlLoader(sc, conf.trainLocation).sample(false, 0.2, seed = 0l).repartitionAndSortWithinPartitions(
      new Partitioner {
        override def numPartitions = conf.numParts
        override def getPartition(key: Any) = {
          val id = key.asInstanceOf[String]
          math.abs(MurmurHash3.stringHash(id)) % conf.numParts
        }
      }).map{ case (id, doc) => RegexTask(id, doc)}.cache()

    val numDocs = commoncrawl.count()
    logInfo(s"loaded $numDocs docs")


    conf.warmup.foreach { warmupCount =>
      logInfo("Warming up!")
      commoncrawl.mapPartitionsWithIndex {
        case (pid, it) =>
          it.take(warmupCount).map { task =>
            val ops = RegexFactoryContainer.factories.indices.map(i => regexMatcher(RegexContainer(i, conf.regex), _: RegexTask))
            ops.foreach {
              x => x.apply(task)
            }
          }
      }.count()
    }

    val banditResults = commoncrawl.mapPartitionsWithIndex {
      case (pid, it) =>

        it.zipWithIndex.map {
          case (task, index) =>
            val startTime = System.nanoTime()
            val (res, action) = bandit.applyAndOutputReward(task)
            val endTime = System.nanoTime()

            s"$pid,$index,${task.id},${task.doc.length},$startTime,$endTime,${action.arm},${action.reward},${conf.regex},${res.length},${'"' + conf.policy + '"'},${'"' + conf.nonstationarity + '"'},${conf.driftDetectionRate},${conf.driftCoefficient},${conf.clusterCoefficient},${conf.communicationRate}"
        }
    }.collect()

    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(conf.outputLocation)))
    writer.write("partition_id,pos_in_partition,canonical_tuple_id,doc_length,system_nano_start_time,system_nano_end_time,arm,reward,regex,num_matches,policy,nonstationarity,driftRate,driftCoefficient,clusterCoefficient,communicationRate\n")
    for (x <- banditResults) {
      writer.write(x + "\n")
    }
    writer.close()
  }

  case class PipelineConfig(
      trainLocation: String = "",
      outputLocation: String = "",
      policy: String = "",
      regex: Int = 0,
      nonstationarity: String = "stationary",
      communicationRate: String = "500ms",
      clusterCoefficient: String = "1.0",
      driftDetectionRate: String = "10s",
      driftCoefficient: String = "1.0",
      warmup: Option[Int] = None,
      numParts: Int = 64)

  def parse(args: Array[String]): PipelineConfig = new OptionParser[PipelineConfig](appName) {
    head(appName, "0.1")
    help("help") text("prints this usage text")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("outputLocation") required() action { (x,c) => c.copy(outputLocation=x) }
    opt[String]("policy") required() action { (x,c) => c.copy(policy=x) }
    opt[String]("regex") required() action { (x,c) => c.copy(regex=x.toInt) }
    opt[String]("nonstationarity") action { (x,c) => c.copy(nonstationarity=x) }
    opt[String]("communicationRate") action { (x,c) => c.copy(communicationRate=x) }
    opt[String]("clusterCoefficient") action { (x,c) => c.copy(clusterCoefficient=x) }
    opt[String]("driftDetectionRate") action { (x,c) => c.copy(driftDetectionRate=x) }
    opt[String]("driftCoefficient") action { (x,c) => c.copy(driftCoefficient=x) }
    opt[Int]("warmup") action { (x,c) => c.copy(warmup=Some(x)) }
    opt[Int]("numParts") action { (x,c) => c.copy(numParts=x) }
  }.parse(args, PipelineConfig()).get

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   *
   * @param args
   */
  def main(args: Array[String]) = {
    val regexes: Seq[String] = Seq("[(http(s)?):\\/\\/(www\\.)?a-zA-Z0-9@:%._\\+~#=]{2,255}\\.[a-z]{2,6}",
      "([A-Za-z]+[ \\t\\n\\r]+[A-Za-z]+[ \\t\\n\\r]+[A-Za-z]+)",
      "<(a)[ \\t\\n\\r]([^>]+[ \\t\\n\\r])?href[ \\t\\n\\r]*=[ \\t\\n\\r]*(\"[^\"]*\"|'[^']*')[^>]*>",
      "(\\+?([0-9]{1,3}))?([-. (]*([0-9]{3})[-. )]*)?(([0-9]{3})[-. ]*([0-9]{2,4})([-.x ]*([0-9]+))?)",
      "[a-zA-Z0-9]+((\\.|_)[A-Za-z0-9!#$%&'*+/=?^`~-]+)*@(?!([a-zA-Z0-9]*\\.[a-zA-Z0-9]*\\.[a-zA-Z0-9]*\\.))([A-Za-z0-9]([a-zA-Z0-9-]*[A-Za-z0-9])?\\.)+[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?",
    "[a-z0-9!#$%&'*+/=?^_`{|}~-]+(\\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@([a-z0-9]([a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9]([a-z0-9-]*[a-z0-9])?",
    "\\$([1-9][0-9]{0,2}|[0-9])(\\,[0-9]{3})*(\\.[0-9]+)?(?=[ \\t\\n\\r]|$)", // Find dollar counts
      "#([a-fA-F\\d]{3}){1,2}|[rR][gG][bB]\\(((\\s*0*(25[0-5]|2[0-4]\\d|1?\\d?\\d)\\s*,){2}\\s*0*(25[0-5]|2[0-4]\\d|1?\\d?\\d)|\\s*0*(100(\\.0+)?|\\d?\\d(\\.\\d+)?)%(\\s*,\\s*0*(100(\\.0+)?|\\d?\\d(\\.\\d+)?)%){2})\\s*\\)|hsl\\(\\s*0*(360|3[0-5]\\d|[12]?\\d?\\d)\\s*(,\\s*0*(100(\\.0+)?|\\d?\\d(\\.\\d+)?)%\\s*){2}\\)|([rR][gG][bB][aA]\\(((\\s*0*(25[0-5]|2[0-4]\\d|1?\\d?\\d)\\s*,){3}|(\\s*0*(100(\\.0+)?|\\d?\\d(\\.\\d+)?)%\\s*,){3})|[hH][sS][lL][aA]\\(\\s*0*(360|3[0-5]\\d|[12]?\\d?\\d)\\s*(,\\s*0*(100(\\.0+)?|\\d?\\d(\\.\\d+)?)%\\s*){2},)\\s*0*(1|0(\\.\\d+)?)\\s*\\)" // CSS colors regexr: http://regexr.com/38lmo slightly edited to be case insensitive & not use word boundaries
    )

    val x =new ComBasistechTclRegexFactory
    val y = x.create(regexes(7))
    val z = y.getMatches("Should match 3 ou 6 digits hexadecimal numbers\n#fff #FFF #Fff #ff004B\nAll numerical values can have leading zeros. Percentages can have decimals.\nWhite spaces are allowed before and after values.\nShould match rgb() colors with 3 values all between 0 and 255\nrgb(0,0,0) RGB(124, 100, 0) rgb(255,255,255) Rgb( 0255, 00001, 02)\nShould match rgb() colors using percentages all between 0 and 100\nrgb(10%,10%,10%) rgb(100.0%, 2.5%, 0%) rgb(00010%, 0002%, 001%)\nShould match rgba() colors with 3 values between 0 and 255 plus 1 decimal value between 0 and 1\nrgba(255 , 0 , 0, 0.5 ) rgba(1,1,1,0.255) rgba(0,0,0,0)\nShould match rgba() colors using 3 percentage values between 0 and 100 plus 1 decimal value between 0 and 1\nrgba(10%,10% , 10%, 0.2) rgba(10%, 0025.2%, 1%, 0.0001)\nShould match hsl() colors with first value between 0 and 360 and 2 more percentage values between 0 and 100\nhsl(0,20%,100%) HsL(360,10% , 0.2% ) hsl(000350, 002%, 0004.1%)\nShould match hsl() colors with first value between 0 and 360 and 2 more percentage values between 0 and 100 plus 1 decimal value between 0 and 1\nhsla(140,2%,50%,0.2) hsla(0,0%,0%,0) hsla(001,002%,00001.2%,0000.254)\n\nShould NOT match hexadecimal numbers with not exactly 3 or 6 digits\n#f #ff #ffff #fffff #fffffff\nShould NOT match invalid hexadecimal values\n#ffg #fffffg\nShould NOT allow space between function and opening parenthesis\nrgb (0,0,0) rgba (0,0,0,0) rgb (0%,0%,0%) rgba (0%,0%,0%,0) hsl (0,0%,0%) hsla (0,0%,0%,0)\nShould NOT match rgb() nor hsl() colors with more or less than 3 values\nrgb(0,0,0,0) rgb(0,0) hsl(0,0%,0%,0) hsl(0,0%)\nShould NOT match rgba() nor hsla() colors with more or less than 4 values\nrgba(0,0,0) rgba(0,0,0,0,0) hsla(0,0%,0%) hsla(0,0%,0%,0,0)\nShould NOT allow rgb values over 255, nor rgb values with decimals\nrgb(256,0,0) rgb(100.2,0,0)\nShould NOT allow percentages over 100\nrgb(120%,10%,1%) hsl(200, 101%, 10%)\nShould NOT allow alpha values over 1\nrgba(0,0,0,1.2) hsla(120, 50%, 50%, 1.3)\nShould NOT allow hue values over 360\nhsl(361,50%,50%) hsla(361,50%,50%,0.5)\nShould NOT match invalid hsl format\nhsl(1%,2%,3%) hsl(5,6,7)\nShould NOT match rgb() colors with mixed percentages and integers\nrgb(255, 10%, 0) rgb(10%, 255, 0) rgba(10%, 255, 0, 0.3)", Array(0)).asScala.toArray
    val appConfig = parse(args)

    val conf = new SparkConf().setAppName(s"$appName-${appConfig.policy}-${appConfig.communicationRate}").set(
      "spark.bandits.communicationRate",
      appConfig.communicationRate)
      .set("spark.bandits.driftDetectionRate", appConfig.driftDetectionRate)
      .set("spark.bandits.alwaysShare", "true")

    conf.setIfMissing("spark.master", "local[4]")
    val sc = new SparkContext(conf)
    run(sc, appConfig)

    sc.stop()
  }
}
