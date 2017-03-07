package keystoneml.pipelines

import breeze.linalg._
import breeze.numerics._
import keystoneml.nodes.images.{Convolver, ImageVectorizer, Windower}
import keystoneml.nodes.learning.{ZCAWhitener, ZCAWhitenerEstimator}
import keystoneml.nodes.stats.Sampler
import keystoneml.utils.{Image, ImageUtils, MatrixUtils, Stats}
import keystoneml.workflow.{Identity, Pipeline}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser


object RandomPatchFlickr extends Serializable with Logging {
  val appName = "RandomPatchCifar"

  def run(sc: SparkContext, conf: RandomCifarConfig): Pipeline[Image, Image] = {
    //Set up some constants.
    val whitenerSize = 1000

    val imgs = sc.binaryFiles(conf.trainLocation).map(_._2).sample(false, 0.1).flatMap(stream => {
      val x = stream.open()
      val img = ImageUtils.loadImage(x).get
      x.close()
      val quarter = ImageUtils.crop(img, 0, 0, img.metadata.xDim/4, img.metadata.yDim/4)
      val half = ImageUtils.crop(img, 0, 0, img.metadata.xDim/2, img.metadata.yDim/2)
      Iterator(quarter, half, img)
    }).repartition(8).cache()

    imgs.count()

    logInfo("Loaded images!")
    val patchExtractor = new Windower(conf.patchSteps, conf.patchSize)
      .andThen(ImageVectorizer.apply)
      .andThen(new Sampler(whitenerSize))

    val (filters, whitener): (DenseMatrix[Double], ZCAWhitener) = {
      val baseFilters = patchExtractor(imgs)
      val baseFilterMat = Stats.normalizeRows(MatrixUtils.rowsToMatrix(baseFilters), 10.0)
      val whitener = new ZCAWhitenerEstimator(eps=conf.whiteningEpsilon).fitSingle(baseFilterMat)

      //Normalize them.
      val sampleFilters = MatrixUtils.sampleRows(baseFilterMat, conf.numFilters)
      val unnormFilters = whitener(sampleFilters)
      val unnormSq = pow(unnormFilters, 2.0)
      val twoNorms = sqrt(sum(unnormSq(*, ::)))

      ((unnormFilters(::, *) / (twoNorms + 1e-10)) * whitener.whitener.t, whitener)
    }

    logInfo("GOT Filters!")



    imgs.map { img =>
      val convs = new Convolver(filters, img.metadata.xDim, img.metadata.yDim, img.metadata.numChannels, normalizePatches = false)
      convs(img)
    }.count()

    logInfo("FINISHED!!")

    //val meme = imgs.first()
    //val pipe = new Convolver(null, imageSize, imageSize, numChannels, None, true) andThen Identity()

    Identity[Image]() andThen Identity()//pipe
  }

  case class RandomCifarConfig(
      trainLocation: String = "",
      testLocation: String = "",
      numFilters: Int = 5,
      whiteningEpsilon: Double = 0.1,
      patchSize: Int = 15,
      patchSteps: Int = 1,
      poolSize: Int = 14,
      poolStride: Int = 13,
      alpha: Double = 0.25,
      lambda: Option[Double] = None,
      sampleFrac: Option[Double] = None)

  def parse(args: Array[String]): RandomCifarConfig = new OptionParser[RandomCifarConfig](appName) {
    head(appName, "0.1")
    help("help") text("prints this usage text")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[Double]("whiteningEpsilon") action { (x,c) => c.copy(whiteningEpsilon=x) }
    opt[Int]("numFilters") action { (x,c) => c.copy(numFilters=x) }
    opt[Int]("patchSize") action { (x,c) => c.copy(patchSize=x) }
    opt[Int]("patchSteps") action { (x,c) => c.copy(patchSteps=x) }
    opt[Int]("poolSize") action { (x,c) => c.copy(poolSize=x) }
    opt[Double]("alpha") action { (x,c) => c.copy(alpha=x) }
    opt[Double]("lambda") action { (x,c) => c.copy(lambda=Some(x)) }
    opt[Double]("sampleFrac") action { (x,c) => c.copy(sampleFrac=Some(x)) }
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
