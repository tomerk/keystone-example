package keystoneml.nodes

import breeze.linalg.DenseMatrix
import keystoneml.nodes.images.Convolver
import keystoneml.utils.{Image, ImageMetadata, ImageUtils, RowMajorArrayVectorizedImage}
import keystoneml.workflow.Transformer

object LoopConvolver {
  /**
   * User-friendly constructor interface for the Conovler.
   *
   * @param filters An array of images with which we convolve each input image. These images should *not* be pre-whitened.
   * @param flipFilters Should the filters be flipped before convolution is applied (used for comparability to MATLAB's
   *                    convnd function.)
   */
  def apply(filters: Array[Image],
            flipFilters: Boolean = false): LoopConvolver = {

    //If we are told to flip the filters, invert their indexes.
    val filterImages = if (flipFilters) {
      filters.map(ImageUtils.flipImage)
    } else filters

    //Pack the filter array into a dense matrix of the right format.
    val packedFilters = Convolver.packFilters(filterImages)

    LoopConvolver(packedFilters)
  }
}

/**
 * A zero-padded convolution
 */
case class LoopConvolver(filters: DenseMatrix[Double]) extends Transformer[Image, Image] {

  val convolutions = filters.t

  def apply(data: Image): Image = {
    val imgWidth = data.metadata.xDim
    val imgHeight = data.metadata.yDim
    val imgChannels = data.metadata.numChannels

    val convSize = math.sqrt(filters.cols/imgChannels).toInt
    val numConvolutions = convolutions.cols

    val resWidth = imgWidth - convSize + 1
    val resHeight = imgHeight - convSize + 1

    val convRes = DenseMatrix.zeros[Double](resWidth*resHeight, numConvolutions)
    var x, y, channel, convNum, convX, convY, cell = 0
    while (convNum < numConvolutions) {
      convX = 0
      while (convX < convSize) {
        convY = 0
        while (convY < convSize) {
          channel = 0
          while (channel < imgChannels) {
            val kernelPixel = convolutions(channel+convX*imgChannels+convY*imgChannels*convSize, convNum)
            y = 0
            while (y < resHeight) {
              x = 0
              while (x < resWidth) {
                val imgPixel = data.get(x + convX, y + convY, channel)
                convRes(y*resWidth+x, convNum) += kernelPixel*imgPixel
                x += 1
              }
              y += 1
            }
            channel += 1
          }
          convY += 1
        }
        convX += 1
      }
      convNum += 1
    }

    val res = new RowMajorArrayVectorizedImage(
      convRes.data,
      ImageMetadata(resWidth, resHeight, numConvolutions))

    res
  }
}
