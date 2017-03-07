package keystoneml.nodes

import breeze.linalg.DenseMatrix
import breeze.math.Complex
import breeze.signal.{fft, ifft}
import keystoneml.nodes.images.Convolver
import keystoneml.pipelines.Logging
import keystoneml.utils.{Image, ImageMetadata, ImageUtils, RowMajorArrayVectorizedImage}
import keystoneml.workflow.Transformer

object FFTConvolver extends Logging {
  /**
   * User-friendly constructor interface for the Conovler.
   *
   * @param filters An array of images with which we convolve each input image. These images should *not* be pre-whitened.
   * @param flipFilters Should the filters be flipped before convolution is applied (used for comparability to MATLAB's
   *                    convnd function.)
   */
  def apply(filters: Array[Image],
            flipFilters: Boolean = false): FFTConvolver = {

    //If we are told to flip the filters, invert their indexes.
    val filterImages = if (flipFilters) {
      filters.map(ImageUtils.flipImage)
    } else filters

    //Pack the filter array into a dense matrix of the right format.
    val packedFilters = Convolver.packFilters(filterImages)

    FFTConvolver(packedFilters, filters.head.metadata.xDim, filters.head.metadata.numChannels)
  }
}


/**
 * An FFT-based convolution
 */
case class FFTConvolver(filters: DenseMatrix[Double], filterSize: Int, numChannels: Int) extends Transformer[Image, Image] {

  // TODO FIXME: This is a buggy unpacking of filters!!!
  val imgFilters = filters.t
    .toArray
    .grouped(filterSize*filterSize*numChannels)
    .map(x => new RowMajorArrayVectorizedImage(x, ImageMetadata(filterSize, filterSize, numChannels)))
    .toArray

  def apply(data: Image): Image = {
    convolve2dFft(data, imgFilters)
  }

  def convolve2dFfts(x: DenseMatrix[Complex], m: DenseMatrix[Complex], origRows: Int, origCols: Int) = {
    //This code based (loosely) on the MATLAB Code here:
    //Assumes you've already computed the fft of x and m.
    //http://www.mathworks.com/matlabcentral/fileexchange/31012-2-d-convolution-using-the-fft

    //Step 1: Pure fucking magic.
    val res = ifft(x :* m).map(_.real)

    //Step 2: the output we care about is in the bottom right corner.
    val startr = origRows - 1
    val startc = origCols - 1
    res(startr until x.rows, startc until x.cols).copy
  }

  def getChannelMatrix(in: Image, c: Int): DenseMatrix[Double] = {
    val dat = (0 until in.metadata.yDim).flatMap(y => (0 until in.metadata.xDim).map(x => in.get(x, y, c))).toArray
    new DenseMatrix[Double](in.metadata.xDim, in.metadata.yDim, dat)
  }

  def addChannelMatrix(in: Image, c: Int, m: DenseMatrix[Double]) = {
    var x = 0
    while (x < in.metadata.xDim) {
      var y = 0
      while (y < in.metadata.yDim) {
        in.put(x, y, c, in.get(x, y, c) + m(x, y))
        y += 1
      }
      x += 1
    }
  }

  def padMat(m: DenseMatrix[Double], nrows: Int, ncols: Int): DenseMatrix[Double] = {
    val res = DenseMatrix.zeros[Double](nrows, ncols)
    res(0 until m.rows, 0 until m.cols) := m
    res
  }

  /**
   * Convolves an n-dimensional image with a k-dimensional
   *
   * @param x
   * @param m
   * @return
   */
  def convolve2dFft(x: Image, m: Array[_ <: Image]): Image = {
    val mx = x.metadata.xDim - m.head.metadata.xDim + 1
    val my = x.metadata.yDim - m.head.metadata.yDim + 1
    val chans = x.metadata.numChannels

    val ressize = mx * my * m.length

    val res = new RowMajorArrayVectorizedImage(Array.fill(ressize)(0.0), ImageMetadata(mx, my, m.length))

    val start = System.currentTimeMillis
    val fftXs = (0 until chans).map(c => fft(getChannelMatrix(x, c)))
    val fftMs = (0 until m.length).map(f => (0 until chans).map(c => fft(padMat(getChannelMatrix(m(f), c), x.metadata.xDim, x.metadata.yDim)))).toArray

    //logInfo(s"Length of Xs: ${fftXs.length}, Length of each m: ${fftMs.first.length}, Total ms: ${fftMs.length}")
    var c = 0
    while (c < chans) {
      var f = 0
      while (f < m.length) {
        val convBlock = convolve2dFfts(fftXs(c), fftMs(f)(c), m(f).metadata.xDim, m(f).metadata.yDim)
        addChannelMatrix(res, f, convBlock) //todo - this could be vectorized.
        f += 1
      }
      c += 1
    }
    res
  }
}
