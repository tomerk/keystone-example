package keystoneml.pipelines

import java.io.File

import breeze.linalg._
import breeze.numerics._
import keystoneml.nodes.FastWindower
import keystoneml.nodes.images.{ImageVectorizer, Windower}
import keystoneml.nodes.learning.{ZCAWhitener, ZCAWhitenerEstimator}
import keystoneml.nodes.stats.Sampler
import keystoneml.utils.{Image, ImageUtils, MatrixUtils, Stats}
import keystoneml.workflow.{Identity, Pipeline}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser


object GenerateRandomFlickrPatches extends Serializable with Logging {
  val appName = "RandomPatchCifar"

  def run(sc: SparkContext, conf: RandomCifarConfig): Pipeline[Image, Image] = {
    //Set up some constants.
    val whitenerSize = 5000

    val imgs = sc.binaryFiles(conf.trainLocation).map(_._2).sample(false, 0.1).map(stream => {
      val x = stream.open()
      val img = ImageUtils.loadImage(x).get
      x.close()
      img
    }).repartition(8).cache()

    imgs.count()

    logInfo("Loaded images!")
    for (patchSize <- conf.minPatchSize to conf.maxPatchSize) {
      logInfo(s"Extracting & Whitening ${conf.numFilters} patches of size $patchSize")
      val patchExtractor = new FastWindower(conf.patchSteps*3, patchSize)
        .andThen(ImageVectorizer.apply)
        .andThen(new Sampler(whitenerSize))

      val (filters, whitener): (DenseMatrix[Double], ZCAWhitener) = {
        val baseFilters = patchExtractor(imgs)
        val baseFilterMat = Stats.normalizeRows(MatrixUtils.rowsToMatrix(baseFilters), 10.0)
        val whitener = new ZCAWhitenerEstimator(eps = conf.whiteningEpsilon).fitSingle(baseFilterMat)

        //Normalize them.
        val sampleFilters = MatrixUtils.sampleRows(baseFilterMat, conf.numFilters)
        val unnormFilters = whitener(sampleFilters)
        val unnormSq = pow(unnormFilters, 2.0)
        val twoNorms = sqrt(sum(unnormSq(*, ::)))

        ((unnormFilters(::, *) / (twoNorms + 1e-10)) * whitener.whitener.t, whitener)
      }

      logInfo(s"Saving ${conf.numFilters} patches of size $patchSize")
      csvwrite(new File(s"${conf.outputLocation}/${conf.numFilters}_patches_size_$patchSize.csv"), filters)
    }

    logInfo("GOT Filters!")

    Identity[Image]() andThen Identity()
  }

  case class RandomCifarConfig(
      trainLocation: String = "",
      outputLocation: String = "",
      numFilters: Int = 30,
      whiteningEpsilon: Double = 0.1,
      minPatchSize: Int = 3,
      maxPatchSize: Int = 30,
      patchSteps: Int = 1)

  def parse(args: Array[String]): RandomCifarConfig = new OptionParser[RandomCifarConfig](appName) {
    head(appName, "0.1")
    help("help") text("prints this usage text")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("outputLocation") required() action { (x,c) => c.copy(outputLocation=x) }
    opt[Double]("whiteningEpsilon") action { (x,c) => c.copy(whiteningEpsilon=x) }
    opt[Int]("numFilters") action { (x,c) => c.copy(numFilters=x) }
    opt[Int]("minPatchSize") action { (x,c) => c.copy(minPatchSize=x) }
    opt[Int]("maxPatchSize") action { (x,c) => c.copy(maxPatchSize=x) }
    opt[Int]("patchSteps") action { (x,c) => c.copy(patchSteps=x) }
  }.parse(args, RandomCifarConfig()).get

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   *
   * @param args
   */
  def main(args: Array[String]) = {
    val appConfig = parse(args)

    val conf = new SparkConf().setAppName(appName)
    conf.setIfMissing("spark.master", "local[4]")
    val sc = new SparkContext(conf)
    run(sc, appConfig)

    sc.stop()
  }
}
