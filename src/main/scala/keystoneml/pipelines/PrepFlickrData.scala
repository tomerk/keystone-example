package keystoneml.pipelines

import java.io.File

import breeze.linalg._
import breeze.numerics._
import keystoneml.loaders.FlickrLoader
import keystoneml.nodes.{FFTConvolver, FastWindower, LoopConvolver}
import keystoneml.nodes.images.{Convolver, ImageVectorizer}
import keystoneml.nodes.learning.{ZCAWhitener, ZCAWhitenerEstimator}
import keystoneml.nodes.stats.Sampler
import keystoneml.utils.{Image, ImageUtils, MatrixUtils, Stats}
import keystoneml.workflow.{Identity, Pipeline}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

case class Crop(startX: Double, startY: Double, endX: Double, endY: Double)

case class Filters(patchSize: Int, numFilters: Int)

case class ConvolutionTask(id: String,
                           image: Image,
                           filters: DenseMatrix[Double])
object PrepFlickrData extends Serializable with Logging {

  def matMultConvolve(task: ConvolutionTask): Image = {
    val imgInfo = task.image.metadata
    new Convolver(
      task.filters,
      imgInfo.xDim,
      imgInfo.yDim,
      imgInfo.numChannels,
      normalizePatches = false).apply(task.image)
  }

  def fftConvolve(task: ConvolutionTask): Image = {
    val imgInfo = task.image.metadata
    new FFTConvolver(
      task.filters,
      math.sqrt(task.filters.cols).toInt,
      imgInfo.numChannels).apply(task.image)
  }

  def loopConvolve(task: ConvolutionTask): Image = {
    new LoopConvolver(task.filters).apply(task.image)
  }


  val appName = "PrepFlickrData"

  def run(sc: SparkContext, conf: RandomCifarConfig): Pipeline[Image, Image] = {
    //Set up some constants.
    val convolutionOps = Seq(loopConvolve(_), matMultConvolve(_), fftConvolve(_))

    val crops = Seq(
      Crop(0, 0, 1, 1),
      Crop(0, 0, 0.5, 0.5),
      Crop(0, 0.5, 0.5, 1.0),
      Crop(0.5, 0, 1.0, 0.5),
      Crop(0.5, 0.5, 1.0, 1.0)
    )

    val filterMetaData = Seq[Filters](Filters(3, 2), Filters(6, 2), Filters(9, 2), Filters(12, 2), Filters(15, 2))
    val filters = filterMetaData.map {
      case Filters(patchSize, numFilters) =>
        val patches = csvread(new File(s"${conf.patchesLocation}/30_patches_size_$patchSize.csv"))
        patches(0 until numFilters,::)
    }

    val imgs = FlickrLoader(sc, conf.trainLocation)
    val croppedImgs = imgs.flatMap{
      case (id, img) => crops.iterator.map {
        case Crop(0, 0, 1, 1) => (id, img)
        case Crop(startX, startY, endX, endY) =>
          (s"$id-cropped-$startX-$startY-$endX-$endY", ImageUtils.crop(
            img,
            (img.metadata.xDim * startX).toInt,
            (img.metadata.yDim * startY).toInt,
            (img.metadata.xDim * endX).toInt,
            (img.metadata.yDim * endY).toInt
          ))
    }}

    val convolutionTasks = croppedImgs.flatMap {
      case (id, img) => filters.iterator.map {
        patches => ConvolutionTask(s"$id-filters-${patches.rows}-${patches.cols}", img, patches)
      }
    }

    convolutionTasks.sample(withReplacement = false, 0.01, seed=0).repartition(conf.numParts).cache()

    convolutionTasks.count()

    logInfo("Loaded images!")
    /*for (patchSize <- conf.minPatchSize to conf.maxPatchSize) {
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

    logInfo("GOT Filters!")*/

    Identity[Image]() andThen Identity()
  }

  case class RandomCifarConfig(
      trainLocation: String = "",
      patchesLocation: String = "",
      numFilters: Int = 30,
      whiteningEpsilon: Double = 0.1,
      minPatchSize: Int = 3,
      maxPatchSize: Int = 30,
      patchSteps: Int = 1,
      numParts: Int = 64)

  def parse(args: Array[String]): RandomCifarConfig = new OptionParser[RandomCifarConfig](appName) {
    head(appName, "0.1")
    help("help") text("prints this usage text")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("patchesLocation") required() action { (x,c) => c.copy(patchesLocation=x) }
    opt[Double]("whiteningEpsilon") action { (x,c) => c.copy(whiteningEpsilon=x) }
    opt[Int]("numFilters") action { (x,c) => c.copy(numFilters=x) }
    opt[Int]("minPatchSize") action { (x,c) => c.copy(minPatchSize=x) }
    opt[Int]("maxPatchSize") action { (x,c) => c.copy(maxPatchSize=x) }
    opt[Int]("patchSteps") action { (x,c) => c.copy(patchSteps=x) }
    opt[Int]("numParts") action { (x,c) => c.copy(numParts=x) }
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
