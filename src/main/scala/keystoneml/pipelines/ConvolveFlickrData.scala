package keystoneml.pipelines

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}

import breeze.linalg._
import keystoneml.loaders.FlickrLoader
import keystoneml.nodes.images.Convolver
import keystoneml.nodes.{FFTConvolver, LoopConvolver}
import keystoneml.utils.{Image, ImageUtils}
import keystoneml.workflow.{Identity, Pipeline}
import org.apache.spark.bandit.policies._
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import scopt.OptionParser

case class Crop(startX: Double, startY: Double, endX: Double, endY: Double)

case class Filters(patchSize: Int, numFilters: Int)

case class ConvolutionTask(id: String,
                           image: Image,
                           filters: DenseMatrix[Double])
object ConvolveFlickrData extends Serializable with Logging {

  def features(task: ConvolutionTask): DenseVector[Double] = {
    DenseVector(
      task.image.metadata.xDim,
      task.image.metadata.yDim,
      task.filters.cols,
      task.filters.rows
    )
  }

  def kernelFeatures(task: ConvolutionTask): DenseVector[Double] = {
    DenseVector(
      task.image.metadata.xDim,
      task.image.metadata.yDim,
      task.image.metadata.xDim * task.image.metadata.yDim,
      task.filters.cols,
      task.filters.rows,
      task.image.metadata.xDim * task.image.metadata.yDim *
        (math.log(task.image.metadata.xDim) + math.log(task.image.metadata.yDim)), // fft
      task.image.metadata.xDim * task.image.metadata.yDim * task.filters.cols * task.filters.rows // matrix multiply
    )
  }


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

    val crops = conf.crops.trim.split(':').map { str =>
      val splitStr = str.split(',')
      Crop(splitStr(0).toDouble, splitStr(1).toDouble, splitStr(2).toDouble, splitStr(3).toDouble)
    }

    val filterMetaData = conf.patches.trim.split(':').map { str =>
      Filters(str.split(',')(0).toInt, str.split(',')(1).toInt)
    }
    val filters = filterMetaData.map {
      case Filters(patchSize, numFilters) =>
        val patches = csvread(new File(s"${conf.patchesLocation}/30_patches_size_$patchSize.csv"))
        patches(0 until numFilters,::)
    }

    val bandits = (0 until conf.numParts).map { _ =>
      conf.policy.trim.toLowerCase.split(':') match {
        // Constant Policies
        case Array("constant", arm) =>
          sc.bandit(convolutionOps, ConstantPolicyParams(arm.toInt))

        // Non-contextual policies
        case Array("epsilon-greedy") =>
          sc.bandit(convolutionOps, EpsilonGreedyPolicyParams())
        case Array("epsilon-greedy", epsilon) =>
          sc.bandit(convolutionOps, EpsilonGreedyPolicyParams(epsilon.toDouble))
        case Array("gaussian-thompson-sampling") =>
          sc.bandit(convolutionOps, GaussianThompsonSamplingPolicyParams())
        case Array("gaussian-thompson-sampling", varMultiplier) =>
          sc.bandit(convolutionOps, GaussianThompsonSamplingPolicyParams(varMultiplier.toDouble))
        case Array("ucb1") =>
          sc.bandit(convolutionOps, UCB1PolicyParams())
        case Array("ucb1", rewardRange) =>
          sc.bandit(convolutionOps, UCB1PolicyParams(rewardRange.toDouble))

        // Contextual policies
        case Array("contextual-epsilon-greedy") =>
          sc.contextualBandit(convolutionOps, features, ContextualEpsilonGreedyPolicyParams(4))
        case Array("contextual-epsilon-greedy", epsilon) =>
          sc.contextualBandit(convolutionOps, features, ContextualEpsilonGreedyPolicyParams(4, epsilon.toDouble))
        case Array("linear-thompson-sampling") =>
          sc.contextualBandit(convolutionOps, features, LinThompsonSamplingPolicyParams(4))
        case Array("linear-thompson-sampling", varMultiplier) =>
          sc.contextualBandit(convolutionOps, features, LinThompsonSamplingPolicyParams(4, varMultiplier.toDouble))
        case Array("lin-ucb") =>
          sc.contextualBandit(convolutionOps, features, LinUCBPolicyParams(4))
        case Array("lin-ucb", alpha) =>
          sc.contextualBandit(convolutionOps, features, LinUCBPolicyParams(4, alpha.toDouble))

        // Contextual Kernel policies
        case Array("kernel-contextual-epsilon-greedy") =>
          sc.contextualBandit(convolutionOps, kernelFeatures, ContextualEpsilonGreedyPolicyParams(7))
        case Array("kernel-contextual-epsilon-greedy", epsilon) =>
          sc.contextualBandit(convolutionOps, kernelFeatures, ContextualEpsilonGreedyPolicyParams(7, epsilon.toDouble))
        case Array("kernel-linear-thompson-sampling") =>
          sc.contextualBandit(convolutionOps, kernelFeatures, LinThompsonSamplingPolicyParams(7))
        case Array("kernel-linear-thompson-sampling", varMultiplier) =>
          sc.contextualBandit(convolutionOps, kernelFeatures, LinThompsonSamplingPolicyParams(7, varMultiplier.toDouble))
        case Array("kernel-lin-ucb") =>
          sc.contextualBandit(convolutionOps, kernelFeatures, LinUCBPolicyParams(7))
        case Array("kernel-lin-ucb", alpha) =>
          sc.contextualBandit(convolutionOps, kernelFeatures, LinUCBPolicyParams(7, alpha.toDouble))

        case _ =>
          throw new IllegalArgumentException(s"Invalid policy ${conf.policy}")
      }
    }


    val imgs = FlickrLoader(sc, conf.trainLocation, conf.labelLocation)
    val croppedImgs = imgs.flatMap{
      case (id, img) => crops.iterator.map {
        case Crop(0, 0, 1, 1) => (id.toInt % conf.numParts, (id, img))
        case Crop(startX, startY, endX, endY) =>
          (id.toInt % conf.numParts, (s"$id-cropped-$startX-$startY-$endX-$endY", ImageUtils.crop(
            img,
            (img.metadata.xDim * startX).toInt,
            (img.metadata.yDim * startY).toInt,
            (img.metadata.xDim * endX).toInt,
            (img.metadata.yDim * endY).toInt
          )))
    }}.partitionBy(new Partitioner {
      override def numPartitions = conf.numParts
      override def getPartition(key: Any) = key.asInstanceOf[Int]
    }).map(_._2)

    val convolutionTasks = croppedImgs.flatMap {
      case (id, img) => filters.iterator.map {
        patches => ConvolutionTask(s"$id-filters-${patches.rows}-${patches.cols}", img, patches)
      }
    }.cache()

    convolutionTasks.count()

    logInfo("Loaded images!")

    val banditResults = convolutionTasks.mapPartitionsWithIndex {
      case (pid, it) =>
        val bandit = if (conf.disableMulticore) {
          bandits(pid)
        } else {
          bandits(0)
        }

        it.zipWithIndex.map {
          case (task, index) =>
            val startTime = System.nanoTime()
            val action = bandit.applyAndOutputReward(task)._2
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
      labelLocation: String = "",
      outputLocation: String = "",
      crops: String = "0,0,1,1:0,0,0.5,0.5:0,0.5,0.5,1.0:0.5,0,1,0.5:0.5,0.5,1,1",
      patches: String = "15,2",
      policy: String = "",
      communicationRate: String = "5s",
      disableMulticore: Boolean = false,
      numParts: Int = 64)

  def parse(args: Array[String]): RandomCifarConfig = new OptionParser[RandomCifarConfig](appName) {
    head(appName, "0.1")
    help("help") text("prints this usage text")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("patchesLocation") required() action { (x,c) => c.copy(patchesLocation=x) }
    opt[String]("outputLocation") required() action { (x,c) => c.copy(outputLocation=x) }
    opt[String]("labelLocation") required() action { (x,c) => c.copy(labelLocation=x) }
    opt[String]("policy") required() action { (x,c) => c.copy(policy=x) }
    opt[String]("patches") action { (x,c) => c.copy(patches=x) }
    opt[String]("crops") action { (x,c) => c.copy(crops=x) }
    opt[String]("communicationRate") action { (x,c) => c.copy(communicationRate=x) }
    opt[Unit]("disableMulticore") action { (x,c) => c.copy(disableMulticore=true) }
    opt[Int]("numParts") action { (x,c) => c.copy(numParts=x) }
  }.parse(args, RandomCifarConfig()).get

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   *
   * @param args
   */
  def main(args: Array[String]) = {
    val appConfig = parse(args)

    val conf = new SparkConf().setAppName(appName).set(
      "spark.bandits.communicationRate",
      appConfig.communicationRate)
    conf.setIfMissing("spark.master", "local[4]")
    val sc = new SparkContext(conf)
    run(sc, appConfig)

    sc.stop()
  }
}
