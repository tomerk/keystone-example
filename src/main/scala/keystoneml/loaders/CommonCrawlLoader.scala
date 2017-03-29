package keystoneml.loaders

import keystoneml.pipelines.Logging
import keystoneml.utils.{Image, LabeledImage}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.jwat.gzip.GzipReader
import org.jwat.warc.WarcReaderCompressed

import scala.collection.JavaConverters._

/**
 * Helper object to load common crawl data.
 */

object CommonCrawlLoader extends Logging {

  /**
   * Loads common crawl data from @dataPath
   *
   * @param sc SparkContext to use
   * @param dataPath Directory containing tar files (can be a HDFS path). This classes assumes
   *                 that each tar file contains images within a directory. The name of the
   *                 directory is treated as the className.
   */
  def apply(sc: SparkContext, dataPath: String): RDD[String] = {
    val filePathsRDD = ImageLoaderUtils.getFilePathsRDD(sc, dataPath)

    val warcRDD = filePathsRDD.flatMap { fileUri =>
      val filePath = new Path(fileUri)
      val conf = new Configuration(true)
      val fs = FileSystem.get(filePath.toUri(), conf)
      val fStream = fs.open(filePath)
      val gzipReader = new GzipReader(fStream)
      val warcReader = new WarcReaderCompressed(gzipReader)
      warcReader.iterator().asScala.filter{ record =>
        val httpHeader = record.getHttpHeader
        if (httpHeader != null) {
          val contentType = httpHeader.contentType
          if (contentType != null) {
            contentType.contains("text/html")
          } else {
            false
          }
        } else {
          false
        }
      }.map(record => new String(IOUtils.toByteArray(record.getPayload.getInputStream)))
    }

    warcRDD
  }
}
