package keystoneml.nodes

import breeze.linalg.DenseVector
import keystoneml.pipelines.FunctionNode
import keystoneml.utils.{ChannelMajorArrayVectorizedImage, Image, ImageMetadata}
import org.apache.spark.rdd.RDD


/**
 * @param stride How big a step to take between patches.
 * @param windowSize Size of a patch.
 */
class FastWindower(
    stride: Int,
    windowSize: Int) extends FunctionNode[RDD[Image], RDD[Image]] {

  def apply(in: RDD[Image]) = {
    in.flatMap(getImageWindow)
  }

  def getImageWindow(image: Image) = {
    val xDim = image.metadata.xDim
    val yDim = image.metadata.yDim
    val numChannels = image.metadata.numChannels

    // Start at (0,0) in (x, y) and
    (0 until xDim - windowSize + 1 by stride).iterator.flatMap { x =>
      (0 until yDim - windowSize + 1 by stride).iterator.map { y =>
        // Extract the window.
        val pool = new DenseVector[Double](windowSize * windowSize * numChannels)
        val startX = x
        val endX = x + windowSize
        val startY = y
        val endY = y + windowSize

        var c = 0
        while (c < numChannels) {
          var s = startX
          while (s < endX) {
            var b = startY
            while (b < endY) {
              pool(c + (s-startX)*numChannels +
                (b-startY)*(endX-startX)*numChannels) = image.get(s, b, c)
              b = b + 1
            }
            s = s + 1
          }
          c = c + 1
        }
        ChannelMajorArrayVectorizedImage(pool.toArray,
          ImageMetadata(windowSize, windowSize, numChannels))
      }
    }
  }

}
