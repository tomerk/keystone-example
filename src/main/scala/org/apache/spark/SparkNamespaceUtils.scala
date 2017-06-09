package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.ui.scope.RDDOperationGraphContainsStringUtil

/**
 * Created by tomerk11 on 6/8/17.
 */
object SparkNamespaceUtils {
  def matchingStages(spark: SparkSession, stringToMatch: String): Set[Int] = {
    spark.sparkContext.jobProgressListener.completedStages
      .filter(x => RDDOperationGraphContainsStringUtil(x, stringToMatch))
      .map(_.stageId).toSet

  }

  def stageExecutorRunTime(spark: SparkSession, stageIds: Set[Int]): Long = {
    spark.sparkContext.jobProgressListener.completedStages
      .filter(x => stageIds.contains(x.stageId)).map(_.taskMetrics.executorRunTime).sum
  }

}
