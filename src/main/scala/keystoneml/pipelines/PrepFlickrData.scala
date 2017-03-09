package keystoneml.pipelines

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}

import breeze.linalg._
import keystoneml.loaders.FlickrLoader
import keystoneml.nodes.images.Convolver
import keystoneml.nodes.{FFTConvolver, LoopConvolver}
import keystoneml.utils.{Image, ImageUtils}
import keystoneml.workflow.{Identity, Pipeline}
import org.apache.spark.bandit.policies.{EpsilonGreedyPolicyParams, GaussianThompsonSamplingPolicyParams}
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
      math.sqrt(task.filters.cols/imgInfo.numChannels).toInt,
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

    val filterMetaData = Seq[Filters](Filters(3, 2))//Seq[Filters](Filters(3, 2), Filters(6, 2), Filters(9, 2), Filters(12, 2), Filters(15, 2))
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
    }.repartition(conf.numParts).cache()

    convolutionTasks.count()

    val convolutionBandit = sc.bandit(convolutionOps, GaussianThompsonSamplingPolicyParams())

    logInfo("Loaded images!")

    val banditResults = convolutionTasks.mapPartitionsWithIndex {
      case (pid, it) =>
        it.zipWithIndex.map {
          case (task, index) =>
            val startTime = System.nanoTime()
            val action = convolutionBandit.applyAndOutputReward(task)._2
            val endTime = System.nanoTime()

            s"$pid,$index,${task.id},$startTime,$endTime,${action.arm},${action.reward}"
        }
    }.collect()

    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(conf.outputLocation)))
    writer.write("partition_id,pos_in_partition,canonical_tuple_id,system_nano_start_time,system_nano_end_time,arm,reward\n")
    for (x <- banditResults) {
      writer.write(x + "\n")
    }
    writer.close()

    Identity[Image]() andThen Identity()
  }

  case class RandomCifarConfig(
      trainLocation: String = "",
      patchesLocation: String = "",
      outputLocation: String = "",
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
    opt[String]("outputLocation") required() action { (x,c) => c.copy(patchesLocation=x) }
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
