package keystoneml.pipelines.tpcds

import java.io.Serializable

import keystoneml.pipelines.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
 * Generate the TPCDS data with the desired settings.
 */
object TPCDSDataGen extends Serializable with Logging {
  val appName = "tpcds-data-gen"

  case class PipelineConfig(
                             dsdgenLocation: String = "",
                             outputLocation: String = "",
                             scaleFactor: Int = 1,
                             numParts: Int = 64)

  def parse(args: Array[String]): PipelineConfig = new OptionParser[PipelineConfig](appName) {
    head(appName, "0.1")
    help("help") text("prints this usage text")
    opt[String]("dsdgenLocation") required() action { (x,c) => c.copy(dsdgenLocation=x) }
    opt[String]("outputLocation") required() action { (x,c) => c.copy(outputLocation=x) }
    opt[Int]("scaleFactor") action { (x,c) => c.copy(scaleFactor=x) }
    opt[Int]("numParts") action { (x,c) => c.copy(numParts=x) }
  }.parse(args, PipelineConfig()).get

  /**
   * Generate the data
   */
  def run(sc: SparkContext, spark: SparkSession, conf: PipelineConfig): Unit = {
    val scaleFactor = conf.scaleFactor
    // Tables in TPC-DS benchmark used by experiments.
    // dsdgenDir is the location of dsdgen tool installed in your machines.
    val tables = new Tables(spark.sqlContext, conf.dsdgenLocation, scaleFactor)
    // Generate data.
    tables.genData(conf.outputLocation,
      "parquet",
      overwrite = true,
      partitionTables = true,
      useDoubleForDecimal = true,
      partitionByColumns = false,
      clusterByPartitionColumns = false,
      filterOutNullPartitionValues = false,
      numPartitions = conf.numParts)
    // Create metastore tables in a specified database for your data.
    // Once tables are created, the current database will be switched to the specified database.
    //tables.createExternalTables(location, format, databaseName, overwrite)
    // Or, if you want to create temporary tables
    //tables.createTemporaryTables(location, format)
    // Setup TPC-DS experiment
  }

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   *
   * @param args
   */
  def main(args: Array[String]) = {
    val appConfig = parse(args)

    val conf = new SparkConf().setAppName(s"$appName")
      .set("spark.sql.parquet.compression.codec", "snappy")
    conf.setIfMissing("spark.master", "local[4]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    run(sc, spark, appConfig)

    sc.stop()
  }
}
