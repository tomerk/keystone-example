package keystoneml.pipelines

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}
import java.util.Random

import breeze.linalg._
import keystoneml.bandits.{ConstantBandit, OracleBandit}
import keystoneml.loaders.FlickrLoader
import keystoneml.nodes.images.Convolver
import keystoneml.nodes.{FFTConvolver, LoopConvolver}
import keystoneml.utils.{Image, ImageUtils}
import keystoneml.workflow.{Identity, Pipeline}
import org.apache.spark.bandit.policies._
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import scopt.OptionParser

import scala.io.Source

case class Crop(startX: Double, startY: Double, endX: Double, endY: Double)

case class Filters(patchSize: Int, numFilters: Int)

case class ConvolveRecord(partitionId: Int,
                          posInPartition: Int,
                          canonicalTupleId: String,
                          imgXDim: Int,
                          imgYDim: Int,
                          filterRows: Int,
                          filterCols: Int,
                          reward: Double,
                          arm: Int)

case class ConvolutionTask(id: String,
                           image: Image,
                           filters: DenseMatrix[Double],
                          globalIndex: Int,
                          partitionId: Int,
                          indexInPartition: Int,
                          var autoregressiveFeatures: Array[Double])

/*
// When patches = 5,3 everywhere
object DebugCholesky extends Serializable with Logging {
  def kernelFeatures(record: ConvolveRecord): DenseVector[Double] = {
    DenseVector(
      //record.imgXDim,
      //record.imgYDim,
      record.imgXDim * record.imgYDim,
      //record.filterCols,
      //record.filterRows,
      //record.imgXDim * record.imgYDim *
       // (math.log(record.imgXDim) + math.log(record.imgYDim)), // fft
      record.imgXDim * record.imgYDim * record.filterCols * record.filterRows // matrix multiply
    )
  }
}
 */


abstract class Feature extends Serializable {
  def get(task: ConvolutionTask): Double
}

case class Bias() extends Feature {
  override def get(task: ConvolutionTask): Double = 1.0 * 1000.0
}
case class ImageRows() extends Feature {
  override def get(task: ConvolutionTask): Double = task.image.metadata.yDim * 1000.0
}
case class ImageCols() extends Feature {
  override def get(task: ConvolutionTask): Double = task.image.metadata.xDim * 1000.0
}
case class ImageSize() extends Feature {
  override def get(task: ConvolutionTask): Double = task.image.metadata.xDim * task.image.metadata.yDim * 1000.0
}

case class FilterRows() extends Feature {
  override def get(task: ConvolutionTask): Double = task.filters.rows * 1000.0
}
case class FilterCols() extends Feature {
  override def get(task: ConvolutionTask): Double = task.filters.cols * 1000.0
}
case class FFTFeature() extends Feature {
  override def get(task: ConvolutionTask): Double = {
    task.image.metadata.xDim * task.image.metadata.yDim *
      (math.log(task.image.metadata.xDim) + math.log(task.image.metadata.yDim)) * 1000.0
  }
}

case class MatrixMultiplyFeature() extends Feature {
  override def get(task: ConvolutionTask): Double = {
    task.image.metadata.xDim * task.image.metadata.yDim * task.filters.cols * task.filters.rows * 1000.0
  }
}

case class GlobalIndex() extends Feature {
  override def get(task: ConvolutionTask): Double = task.globalIndex
}
case class PosInPartition() extends Feature {
  override def get(task: ConvolutionTask): Double = task.indexInPartition
}
case class Periodic(period: Int) extends Feature {
  override def get(task: ConvolutionTask): Double = task.indexInPartition % period
}

object ConvolveFlickrData extends Serializable with Logging {

  def makeFeatures(features: String): (ConvolutionTask => DenseVector[Double], Int) = {
    val featureList: Array[Feature] = features.split(',').map {
      case "bias" => Bias()
      case "image_rows" => ImageRows()
      case "image_cols" => ImageCols()
      case "image_size" => ImageSize()
      case "filter_rows" => FilterRows()
      case "filter_cols" => FilterCols()
      case "fft_cost_model" => FFTFeature()
      case "matrix_multiply_cost_model" => MatrixMultiplyFeature()
      case "global_index" => GlobalIndex()
      case "pos_in_partition" => PosInPartition()
      case periodicString if periodicString.startsWith("periodic_") =>
        val period = periodicString.split('_').last.toInt
        Periodic(period)
      case unknown => throw new IllegalArgumentException(s"Unknown feature $unknown")
    }

    (extractFeatures(featureList, _), featureList.length)
  }

  def extractFeatures(featureList: Array[Feature], task: ConvolutionTask): DenseVector[Double] = {
    DenseVector(featureList.map(_.get(task)))
  }

  def minOracle(path: String): ConvolutionTask => Int = {
    val records = Source.fromFile(path).getLines.filter(x => !x.startsWith("partition_id")).map {
      line =>
        val splitLine = line.split(',')
        ConvolveRecord(
          partitionId = splitLine(0).toInt,
          posInPartition = splitLine(1).toInt,
          canonicalTupleId = splitLine(2),
          imgXDim = splitLine(3).toInt,
          imgYDim = splitLine(4).toInt,
          filterRows = splitLine(5).toInt,
          filterCols = splitLine(6).toInt,
          reward = splitLine(10).toDouble,
          arm = splitLine(9).toInt)
    }.toSeq

    val armRewards =
      // Group by convolution id
      records.groupBy(_.canonicalTupleId)
      // For each convolution id, group + sum the rewards by arm
      .mapValues(_.groupBy(_.arm).mapValues(_.map(_.reward).sum))

    // we need a map identity here because a mapValues bug means this isn't serializable
    val bestArms = armRewards.mapValues(_.maxBy(_._2)._1).map(identity)

    val oracle: ConvolutionTask => Int = task => bestArms(task.id)
    oracle
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
          new ConstantBandit(arm.toInt, convolutionOps(arm.toInt))

        // Oracle Policies
        case Array("oracle", "min", path) =>
          new OracleBandit(minOracle(path), convolutionOps)

        // Non-contextual policies
        case Array("epsilon-greedy") =>
          sc.bandit(convolutionOps, EpsilonGreedyPolicyParams())
        case Array("epsilon-greedy", epsilon) =>
          sc.bandit(convolutionOps, EpsilonGreedyPolicyParams(epsilon.toDouble))
        case Array("gaussian-thompson-sampling") =>
          sc.bandit(convolutionOps, GaussianThompsonSamplingPolicyParams())
        case Array("gaussian-thompson-sampling", varMultiplier) =>
          sc.bandit(convolutionOps, GaussianThompsonSamplingPolicyParams(varMultiplier.toDouble))
        case Array("ucb1-normal") =>
          sc.bandit(convolutionOps, UCB1NormalPolicyParams())
        case Array("ucb1-normal", rewardRange) =>
          sc.bandit(convolutionOps, UCB1NormalPolicyParams(rewardRange.toDouble))
        case Array("ucb-gaussian-bayes") =>
          sc.bandit(convolutionOps, GaussianBayesUCBPolicyParams())
        case Array("ucb-gaussian-bayes", rewardRange) =>
          sc.bandit(convolutionOps, GaussianBayesUCBPolicyParams(rewardRange.toDouble))

        // Contextual policies
        case Array("contextual-epsilon-greedy", featureString) =>
          val (features, numFeatures) = makeFeatures(featureString)
          sc.contextualBandit(convolutionOps, features, ContextualEpsilonGreedyPolicyParams(numFeatures))
        case Array("contextual-epsilon-greedy", featureString, epsilon) =>
          val (features, numFeatures) = makeFeatures(featureString)
          sc.contextualBandit(convolutionOps, features, ContextualEpsilonGreedyPolicyParams(numFeatures, epsilon.toDouble))
        case Array("linear-thompson-sampling", featureString) =>
          val (features, numFeatures) = makeFeatures(featureString)
          sc.contextualBandit(convolutionOps, features, LinThompsonSamplingPolicyParams(numFeatures, 2.0))
        case Array("linear-thompson-sampling", featureString, useCholesky, varMultiplier) =>
          val (features, numFeatures) = makeFeatures(featureString)
          sc.contextualBandit(convolutionOps, features, LinThompsonSamplingPolicyParams(numFeatures, varMultiplier.toDouble, useCholesky = useCholesky.toBoolean))
        case Array("lin-ucb", featureString) =>
          val (features, numFeatures) = makeFeatures(featureString)
          sc.contextualBandit(convolutionOps, features, LinUCBPolicyParams(numFeatures))
        case Array("lin-ucb", featureString, alpha) =>
          val (features, numFeatures) = makeFeatures(featureString)
          sc.contextualBandit(convolutionOps, features, LinUCBPolicyParams(numFeatures, alpha.toDouble))

        case _ =>
          throw new IllegalArgumentException(s"Invalid policy ${conf.policy}")
      }
    }


    val approxPartitionSize = 8000.0 * crops.length / conf.numParts

    val imgs = FlickrLoader(sc, conf.trainLocation, conf.labelLocation)
    val croppedImgs = imgs.flatMap{
      case (id, img) => crops.iterator.zipWithIndex.map {
        case (Crop(0, 0, 1, 1), cropIndex) =>
          ((id.toInt, cropIndex), img)
        case (Crop(startX, startY, endX, endY), cropIndex) =>
          ((id.toInt, cropIndex), ImageUtils.crop(
            img,
            (img.metadata.xDim * startX).toInt,
            (img.metadata.yDim * startY).toInt,
            (img.metadata.xDim * endX).toInt,
            (img.metadata.yDim * endY).toInt
          ))
    }}.repartitionAndSortWithinPartitions(new Partitioner {
      override def numPartitions = conf.numParts
      override def getPartition(key: Any) = {
        val id = key.asInstanceOf[(Int, Int)]
        (id._1 + id._2) % conf.numParts
      }
    })

    val convolutionTasks = conf.nonstationarity.split(',') match {
      case Array("stationary") =>
        croppedImgs.mapPartitionsWithIndex { case (index, it) =>
          val rand = new Random(index)

          it.zipWithIndex.map {
            case ((id, img), indexInPartition) =>
              val globalIndex: Int = (index * approxPartitionSize + indexInPartition).toInt
              val random_index = rand.nextInt(filters.length)
              val patches = filters(random_index)
              ConvolutionTask(s"${id._1}_${id._2}", img, patches, partitionId = index, indexInPartition = indexInPartition, globalIndex = globalIndex, autoregressiveFeatures = null)
          }
        }.cache()

      case Array("sort") =>
        val approxCount = approxPartitionSize * conf.numParts
        croppedImgs.mapPartitionsWithIndex { case (index, it) =>
          it.zipWithIndex.map {
            case ((id, img), indexInPartition) =>
              val globalIndex: Int = (index * approxPartitionSize + indexInPartition).toInt
              val filterPos = math.min(math.round(filters.length * globalIndex / approxCount).toInt, filters.length - 1)
              val patches = filters(filterPos)
              ConvolutionTask(s"${id._1}_${id._2}", img, patches, partitionId = index, indexInPartition = indexInPartition, globalIndex = globalIndex, autoregressiveFeatures = null)
          }
        }.cache()

      case Array("sort_partitions") =>
        croppedImgs.mapPartitionsWithIndex { case (index, it) =>
          it.zipWithIndex.map {
            case ((id, img), indexInPartition) =>
              val globalIndex: Int = (index * approxPartitionSize + indexInPartition).toInt
              val filterPos = math.min(math.round(filters.length * indexInPartition / approxPartitionSize).toInt, filters.length - 1)
              val patches = filters(filterPos)
              ConvolutionTask(s"${id._1}_${id._2}", img, patches, partitionId = index, indexInPartition = indexInPartition, globalIndex = globalIndex, autoregressiveFeatures = null)
          }
        }.cache()

      case Array("periodic") =>
        croppedImgs.mapPartitionsWithIndex { case (index, it) =>
          it.zipWithIndex.map {
            case ((id, img), indexInPartition) =>
              val globalIndex: Int = (index * approxPartitionSize + indexInPartition).toInt
              val patches = filters(indexInPartition % filters.length)
              ConvolutionTask(s"${id._1}_${id._2}", img, patches, partitionId = index, indexInPartition = indexInPartition, globalIndex = globalIndex, autoregressiveFeatures = null)
          }
        }.cache()

      case Array("hash") =>
        require(filters.length >= conf.numParts, "With hash partitioning must have more filter options than partitions")
        croppedImgs.mapPartitionsWithIndex { case (index, it) =>
          val rand = new Random(index)
          val filtersForPartition = filters.zipWithIndex.filter(_._2 % conf.numParts == index).map(_._1)

          it.zipWithIndex.map {
            case ((id, img), indexInPartition) =>
              val globalIndex: Int = (index * approxPartitionSize + indexInPartition).toInt
              val random_index = rand.nextInt(filtersForPartition.length)
              val patches = filtersForPartition(random_index)
              ConvolutionTask(s"${id._1}_${id._2}", img, patches, partitionId = index, indexInPartition = indexInPartition, globalIndex = globalIndex, autoregressiveFeatures = null)
          }
        }.cache()

      case Array("random_walk", probability_string) =>
        val probability = probability_string.toDouble
        croppedImgs.mapPartitionsWithIndex { case (index, it) =>
          var filter_index: Int = filters.length / 2
          val rand = new Random(index)

          it.zipWithIndex.map {
            case ((id, img), indexInPartition) =>
              val globalIndex: Int = (index * approxPartitionSize + indexInPartition).toInt
              val draw = rand.nextFloat()
              if (draw < probability) {
                filter_index += 1
              } else if (draw < probability * 2) {
                filter_index -= 1
              }
              filter_index = math.max(math.min(filter_index, filters.length - 1), 0)
              val patches = filters(filter_index)
              ConvolutionTask(s"${id._1}_${id._2}", img, patches, partitionId = index, indexInPartition = indexInPartition, globalIndex = globalIndex, autoregressiveFeatures = null)
          }
        }.cache()

      case Array("global_random_walk", probability_string) =>
        val probability = probability_string.toDouble
        croppedImgs.mapPartitionsWithIndex { case (index, it) =>
          var filter_index: Int = filters.length / 2
          val rand = new Random(conf.numParts * 10)

          it.zipWithIndex.map {
            case ((id, img), indexInPartition) =>
              val globalIndex: Int = (index * approxPartitionSize + indexInPartition).toInt
              val draw = rand.nextFloat()
              if (draw < probability) {
                filter_index += 1
              } else if (draw < probability * 2) {
                filter_index -= 1
              }
              filter_index = math.max(math.min(filter_index, filters.length - 1), 0)
              val patches = filters(filter_index)
              ConvolutionTask(s"${id._1}_${id._2}", img, patches, partitionId = index, indexInPartition = indexInPartition, globalIndex = globalIndex, autoregressiveFeatures = null)
          }
        }.cache()

      case _ => throw new IllegalArgumentException(s"Unknown nonstationarity ${conf.nonstationarity}")
    }

    convolutionTasks.count()

    logInfo("Loaded images!")
    conf.warmup.foreach { warmupCount =>
      logInfo("Warming up!")
      convolutionTasks.mapPartitionsWithIndex {
        case (pid, it) =>
          if (conf.disableMulticore) {
            bandits(pid)
          } else {
            bandits(0)
          }

          it.take(warmupCount).map { task =>
            fftConvolve(task)
            loopConvolve(task)
            matMultConvolve(task)
          }
      }.count()
    }

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

            s"$pid,$index,${task.id},${task.image.metadata.xDim},${task.image.metadata.yDim},${task.filters.rows},${task.filters.cols},$startTime,$endTime,${action.arm},${action.reward},${'"' + conf.policy + '"'},${'"' + conf.nonstationarity + '"'},${conf.driftDetectionRate},${conf.driftCoefficient},${conf.clusterCoefficient},${conf.communicationRate}"
        }
    }.collect()

    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(conf.outputLocation)))
    writer.write("partition_id,pos_in_partition,canonical_tuple_id,img_x_dim,img_y_dim,filter_rows,filter_cols,system_nano_start_time,system_nano_end_time,arm,reward,policy,nonstationarity,driftRate,driftCoefficient,clusterCoefficient,communicationRate\n")
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
      nonstationarity: String = "stationary",
      communicationRate: String = "5s",
      clusterCoefficient: String = "1.0",
      driftDetectionRate: String = "5s",
      driftCoefficient: String = "1.0",
      disableMulticore: Boolean = false,
      warmup: Option[Int] = None,
      numParts: Int = 64)

  def parse(args: Array[String]): RandomCifarConfig = new OptionParser[RandomCifarConfig](appName) {
    head(appName, "0.1")
    help("help") text("prints this usage text")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("patchesLocation") required() action { (x,c) => c.copy(patchesLocation=x) }
    opt[String]("outputLocation") required() action { (x,c) => c.copy(outputLocation=x) }
    opt[String]("labelLocation") required() action { (x,c) => c.copy(labelLocation=x) }
    opt[String]("policy") required() action { (x,c) => c.copy(policy=x) }
    opt[String]("nonstationarity") action { (x,c) => c.copy(nonstationarity=x) }
    opt[String]("patches") action { (x,c) => c.copy(patches=x) }
    opt[String]("crops") action { (x,c) => c.copy(crops=x) }
    opt[String]("communicationRate") action { (x,c) => c.copy(communicationRate=x) }
    opt[String]("clusterCoefficient") action { (x,c) => c.copy(clusterCoefficient=x) }
    opt[String]("driftDetectionRate") action { (x,c) => c.copy(driftDetectionRate=x) }
    opt[String]("driftCoefficient") action { (x,c) => c.copy(driftCoefficient=x) }
    opt[Unit]("disableMulticore") action { (x,c) => c.copy(disableMulticore=true) }
    opt[Int]("warmup") action { (x,c) => c.copy(warmup=Some(x)) }
    opt[Int]("numParts") action { (x,c) => c.copy(numParts=x) }
  }.parse(args, RandomCifarConfig()).get

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   *
   * @param args
   */
  def main(args: Array[String]) = {
    val appConfig = parse(args)

    val conf = new SparkConf().setAppName(s"$appName-${appConfig.crops}-${appConfig.patches}-${appConfig.policy}-${appConfig.communicationRate}-${appConfig.disableMulticore}").set(
      "spark.bandits.communicationRate",
      appConfig.communicationRate).set(
      "spark.bandits.clusterCoefficient",
      appConfig.clusterCoefficient).set(
      "spark.bandits.driftDetectionRate",
      appConfig.driftDetectionRate).set(
      "spark.bandits.driftCoefficient",
      appConfig.driftCoefficient)
    .set("spark.bandits.contextCombinedWeights", "True")

    conf.setIfMissing("spark.master", "local[4]")
    val sc = new SparkContext(conf)
    run(sc, appConfig)

    sc.stop()
  }
}
