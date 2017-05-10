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

import java.io._

import keystoneml.bandits.{ConstantBandit, OracleBandit}
import keystoneml.pipelines.Logging
import org.apache.spark.bandit.policies._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util._
import scopt.OptionParser

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random

/**
 * Benchmark to measure TPCDS query performance.
 * To run this:
 *  spark-submit --class <this class> --jars <spark sql test jar>
 */
object TPCDSQueryBenchmark extends Serializable with Logging {
  val appName = "tpcds-queries"

  case class PipelineConfig(
                             dataLocation: String = "",
                             outputLocation: String = "",
                             queryGenRules: String = "q49,q72,q75,q78,q80,q93",
                             cacheTables: Boolean = false,
                             confSettings: Array[Array[SparkSetting]] = Array(Array()),
                             policy: String = "",
                             nonstationarity: String = "stationary",
                             communicationRate: String = "5s",
                             clusterCoefficient: String = "1.0",
                             driftDetectionRate: String = "5s",
                             driftCoefficient: String = "1.0",
                             warmup: Option[Int] = None)

  case class SparkSetting(key: String, value: String) {
    def set(spark: SparkSession): Unit = {
      spark.conf.set(key, value)
    }
  }

  def parse(args: Array[String]): PipelineConfig = new OptionParser[PipelineConfig](appName) {
    head(appName, "0.1")
    help("help") text("prints this usage text")
    opt[String]("dataLocation") required() action { (x,c) => c.copy(dataLocation=x) }
    opt[String]("outputLocation") required() action { (x,c) => c.copy(outputLocation=x) }
    opt[String]("policy") required() action { (x,c) => c.copy(policy=x) }
    opt[String]("queries") action { (x,c) => c.copy(queryGenRules=x) }
    opt[Boolean]("cacheTables") action { (x,c) => c.copy(cacheTables=x) }
    opt[String]("confSettings") action { (x,c) => c.copy(confSettings=x.split('-').map{
      x => x.split(',').map {
        setting =>
          val Array(key, value) = setting.split(':')
          SparkSetting(key, value)
      }}) }
    opt[String]("nonstationarity") action { (x,c) => c.copy(nonstationarity=x) }
    opt[String]("communicationRate") action { (x,c) => c.copy(communicationRate=x) }
    opt[String]("clusterCoefficient") action { (x,c) => c.copy(clusterCoefficient=x) }
    opt[String]("driftDetectionRate") action { (x,c) => c.copy(driftDetectionRate=x) }
    opt[String]("driftCoefficient") action { (x,c) => c.copy(driftCoefficient=x) }
    opt[Int]("warmup") action { (x,c) => c.copy(warmup=Some(x)) }
  }.parse(args, PipelineConfig()).get

  val tables = Seq("catalog_page", "catalog_returns", "customer", "customer_address",
    "customer_demographics", "date_dim", "household_demographics", "inventory", "item",
    "promotion", "store", "store_returns", "catalog_sales", "web_sales", "store_sales",
    "web_returns", "web_site", "reason", "call_center", "warehouse", "ship_mode", "income_band",
    "time_dim", "web_page")

  def setupTables(spark: SparkSession, dataLocation: String, cacheTables: Boolean): Map[String, Long] = {
    tables.map { tableName =>
      spark.read.parquet(s"$dataLocation/$tableName").createOrReplaceTempView(tableName)
      if (cacheTables) {
        spark.sqlContext.cacheTable(tableName)
      }
      tableName -> spark.table(tableName).count()
    }.toMap
  }

  def tpcdsAll(spark: SparkSession, dataLocation: String, queries: Seq[String]): Unit = {
    val tableSizes = setupTables(spark, dataLocation, false)

    // List of all TPC-DS queries
    //val tpcdsQueries = Seq("q49", "q72", "q75", "q78", "q80", "q93") //q72 is slow on hash
    // "q77" gives error
    // "q5" and "q40" are only broadcast joins w/ the current scale factor of 5 & 16 partitions
    // "q51" and "q97" don't support hash joins because not a hashable relation
    // All the other queries don't support joins

    val tpcdsQueries =Seq(
      "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
      "q12", "q13", "q14a", "q14b", "q15", "q16", "q17", "q18", "q19", "q20",
      "q21", "q22", "q23a", "q23b", "q24a", "q24b", "q25", "q26", "q27", "q29", "q30",
      "q31", "q32", "q33", "q34", "q35", "q36", "q37", "q38", "q39a", "q39b", "q40",
      "q41", "q42", "q43", "q44", "q45", "q46", "q47", "q48", "q49", "q50",
      "q51", "q52", "q53", "q54", "q55", "q56", "q57", "q58", "q59", "q72", "q75", "q78", "q80", "q93") //q28, q60 gives error atm

    /*val tpcdsQueries =Seq(
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

    Seq("8", "16", "32", "64", "128", "256", "1024", "2048").foreach { numPartitionString =>
      Seq("true", "false").foreach { codeGen =>
        spark.sqlContext.setConf("spark.sql.shuffle.partitions", numPartitionString)
        spark.sqlContext.setConf("spark.sql.codegen.wholeStage", codeGen)
        logInfo(s"About to use $numPartitionString, codeGen $codeGen")
        queries.foreach { name =>
          val queryString = scala.io.Source.fromInputStream(Thread.currentThread().getContextClassLoader
            .getResourceAsStream(s"tpcds/$name.sql")).mkString

          // This is an indirect hack to estimate the size of each query's input by traversing the
          // logical plan and adding up the sizes of all tables that appear in the plan. Note that this
          // currently doesn't take WITH subqueries into account which might lead to fairly inaccurate
          // per-row processing time for those cases.
          val queryRelations = scala.collection.mutable.HashSet[String]()
          spark.sql(queryString).queryExecution.logical.map {
            case ur@UnresolvedRelation(t: TableIdentifier, _) =>
              queryRelations.add(t.table)
            case lp: LogicalPlan =>
              lp.expressions.foreach {
                _ foreach {
                  case subquery: SubqueryExpression =>
                    subquery.plan.foreach {
                      case ur@UnresolvedRelation(t: TableIdentifier, _) =>
                        queryRelations.add(t.table)
                      case _ =>
                    }
                  case _ =>
                }
              }
            case _ =>
          }
          val numRows = queryRelations.map(tableSizes.getOrElse(_, 0L)).sum


          val startTime = System.currentTimeMillis()
          spark.sql(queryString).collect()
          val totalTime = System.currentTimeMillis() - startTime

          logInfo(s"Query $name took $totalTime ms (and had $numRows rows of input)")
        }
      }
    }
  }

  def runQuery(spark: SparkSession, query: String, settings: Array[SparkSetting]): Array[Row] = {
    // Set the appropriate spark configurations
    settings.foreach {
      _.set(spark)
    }

    val queryString = scala.io.Source.fromInputStream(Thread.currentThread().getContextClassLoader
      .getResourceAsStream(s"tpcds/$query.sql")).mkString

    spark.sql(queryString).collect()
  }

  case class SqlTask(query: String, arm: Int, reward: Double)

  def minOracle(path: String): String => Int = {
    val records = Source.fromFile(path).getLines.filter(x => !x.startsWith("index")).map {
      line =>
        val splitLine = line.split(',')
        SqlTask(
          query = splitLine(1),
          arm = splitLine(2).toInt,
          reward = splitLine(3).toDouble
        )
    }.toSeq

    val armRewards =
    // Group by query name
      records.groupBy(_.query)
        // For each convolution id, group + sum the rewards by arm
        // Todo Fixme: This sum is dangerous if arms were used different numbers of times.
        // Should be mean.
        .mapValues(_.groupBy(_.arm).mapValues(_.map(_.reward).sum))

    // we need a map identity here because a mapValues bug means this isn't serializable
    val bestArms = armRewards.mapValues(_.maxBy(_._2)._1).map(identity)

    val oracle: String => Int = query => bestArms(query)
    oracle
  }

  def genQueries(queryGenRules: String): Seq[String] = {
    val seed = 0l
    val random = new Random(seed)

    val queries = ArrayBuffer[String]()
    queryGenRules.split('-').map(_.split(':')).foreach {
      case Array(queryBlock, countString) =>
        val curQueries = queryBlock.split(",")
        for (i <- 0 until countString.toInt) {
          val nextQuery = curQueries(random.nextInt(curQueries.length))
          queries.append(nextQuery)
        }
    }

    queries
  }

  def run(sc: SparkContext, spark: SparkSession, conf: PipelineConfig): Unit = {

    val settingKeys = conf.confSettings.map(_.map(_.key)).head
    require(conf.confSettings.forall(_.map(_.key) sameElements settingKeys),
      "All possible confs must same key order")

    require(conf.dataLocation.nonEmpty,
      "please modify the value of dataLocation to point to your local TPCDS data")

    val arms: Array[String => Array[Row]] = conf.confSettings.map(settings => {
      (query: String) => runQuery(spark, query, settings)
    })

    val bandit = conf.policy.trim.toLowerCase.split(':') match {
      // Constant Policies
      case Array("constant", arm) =>
        new ConstantBandit(arm.toInt, arms(arm.toInt))

      // Oracle Policies
      case Array("oracle", "min", path) =>
        new OracleBandit(minOracle(path), arms)

      // Non-contextual policies
      case Array("epsilon-greedy") =>
        sc.bandit(arms, EpsilonGreedyPolicyParams())
      case Array("epsilon-greedy", epsilon) =>
        sc.bandit(arms, EpsilonGreedyPolicyParams(epsilon.toDouble))
      case Array("gaussian-thompson-sampling") =>
        sc.bandit(arms, GaussianThompsonSamplingPolicyParams())
      case Array("gaussian-thompson-sampling", varMultiplier) =>
        sc.bandit(arms, GaussianThompsonSamplingPolicyParams(varMultiplier.toDouble))
      case Array("ucb1-normal") =>
        sc.bandit(arms, UCB1NormalPolicyParams())
      case Array("ucb1-normal", rewardRange) =>
        sc.bandit(arms, UCB1NormalPolicyParams(rewardRange.toDouble))
      case Array("ucb-gaussian-bayes") =>
        sc.bandit(arms, GaussianBayesUCBPolicyParams())
      case Array("ucb-gaussian-bayes", rewardRange) =>
        sc.bandit(arms, GaussianBayesUCBPolicyParams(rewardRange.toDouble))

      case _ =>
        throw new IllegalArgumentException(s"Invalid policy ${conf.policy}")
    }

    val tableSizes = setupTables(spark, conf.dataLocation, conf.cacheTables)


    val banditResults = genQueries(conf.queryGenRules).zipWithIndex.map { case (query, index) =>
      val startTime = System.nanoTime()
      val action = bandit.applyAndOutputReward(query)._2
      val endTime = System.nanoTime()

      s"$index,$query,${action.arm},${action.reward},${conf.confSettings(action.arm).map(_.value).mkString(",")},$startTime,$endTime,${'"' + conf.policy + '"'},${'"' + conf.queryGenRules + '"'},${conf.driftDetectionRate},${conf.driftCoefficient},${conf.clusterCoefficient},${conf.communicationRate}"
    }

    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(conf.outputLocation)))
    writer.write(s"index,query,arm,reward,${settingKeys.mkString(",")},system_nano_start_time,system_nano_end_time,policy,query_gen_rules,driftRate,driftCoefficient,clusterCoefficient,communicationRate\n")

    for (x <- banditResults) {
      writer.write(x + "\n")
    }
    writer.close()

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
      .set("spark.bandits.driftDetectionRate", "99999999s")
      .set("spark.bandits.alwaysShare", "true")
      .set("spark.bandits.communicationRate", "500ms")

    conf.setIfMissing("spark.master", "local[4]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    run(sc, spark, appConfig)

    sc.stop()
  }

}


