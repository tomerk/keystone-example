/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package keystoneml.pipelines.tpcds

import java.io.{File, Serializable}

import keystoneml.pipelines.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util._
import scopt.OptionParser

/**
 * Benchmark to measure TPCDS query performance.
 * To run this:
 *  spark-submit --class <this class> --jars <spark sql test jar>
 */
object TPCDSQueryBenchmark extends Logging {
  val conf =
    new SparkConf()
      .setMaster("local[4]")
      .setAppName("test-sql-context")
      .set("spark.sql.parquet.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "16")
      .set("spark.driver.memory", "3g")
      .set("spark.executor.memory", "3g")
        //.set("spark.sql.codegen.wholeStage", "false")
      .set("spark.sql.autoBroadcastJoinThreshold", (20 * 1024 * 1024).toString)

  val spark = SparkSession.builder.config(conf).getOrCreate()

  val tables = Seq("catalog_page", "catalog_returns", "customer", "customer_address",
    "customer_demographics", "date_dim", "household_demographics", "inventory", "item",
    "promotion", "store", "store_returns", "catalog_sales", "web_sales", "store_sales",
    "web_returns", "web_site", "reason", "call_center", "warehouse", "ship_mode", "income_band",
    "time_dim", "web_page")

  def setupTables(dataLocation: String): Map[String, Long] = {
    tables.map { tableName =>
      spark.read.parquet(s"$dataLocation/$tableName").createOrReplaceTempView(tableName)
      tableName -> spark.table(tableName).count()
    }.toMap
  }

  def tpcdsAll(dataLocation: String, queries: Seq[String]): Unit = {
    require(dataLocation.nonEmpty,
      "please modify the value of dataLocation to point to your local TPCDS data")
    val tableSizes = setupTables(dataLocation)
    queries.foreach { name =>
      val queryString = fileToString(new File(Thread.currentThread().getContextClassLoader
        .getResource(s"tpcds/$name.sql").getFile))

      val startTime = System.currentTimeMillis()
      spark.sql(queryString).collect()
      val totalTime = System.currentTimeMillis() - startTime

      logInfo(s"Query $name took $totalTime ms")
    }
  }

  def main(args: Array[String]): Unit = {

    // List of all TPC-DS queries
    val tpcdsQueries = Seq("q49", "q72", "q75", "q78", "q80", "q93") //q72 is slow on hash
    // "q77" gives error
    // "q5" and "q40" are only broadcast joins w/ the current scale factor of 5 & 16 partitions
    // "q51" and "q97" don't support hash joins because not a hashable relation
    // All the other queries don't support joins

    /*Seq(
      "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
      "q12", "q13", "q14a", "q14b", "q15", "q16", "q17", "q18", "q19", "q20",
      "q21", "q22", "q23a", "q23b", "q24a", "q24b", "q25", "q26", "q27", "q28", "q29", "q30",
      "q31", "q32", "q33", "q34", "q35", "q36", "q37", "q38", "q39a", "q39b", "q40",
      "q41", "q42", "q43", "q44", "q45", "q46", "q47", "q48", "q49", "q50",
      "q51", "q52", "q53", "q54", "q55", "q56", "q57", "q58", "q59", "q60",
      "q61", "q62", "q63", "q64", "q65", "q66", "q67", "q68", "q69", "q70",
      "q71", "q72", "q73", "q74", "q75", "q76", "q77", "q78", "q79", "q80",
      "q81", "q82", "q83", "q84", "q85", "q86", "q87", "q88", "q89", "q90",
      "q91", "q92", "q93", "q94", "q95", "q96", "q97", "q98", "q99")*/

    // In order to run this benchmark, please follow the instructions at
    // https://github.com/databricks/spark-sql-perf/blob/master/README.md to generate the TPCDS data
    // locally (preferably with a scale factor of 5 for benchmarking). Thereafter, the value of
    // dataLocation below needs to be set to the location where the generated data is stored.
    val dataLocation = "/Users/tomerk11/Desktop/tpcds-data"

    tpcdsAll(dataLocation, queries = tpcdsQueries)
  }
}

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
      true,
      true,
      true,
      false,
      false,
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