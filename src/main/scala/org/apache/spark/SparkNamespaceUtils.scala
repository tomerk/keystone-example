package org.apache.spark

import org.apache.spark.scheduler.TaskInfo
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

  def stageTotalExecutorRunTime(spark: SparkSession, stageIds: Set[Int]): Long = {
    spark.sparkContext.jobProgressListener.completedStages
      .filter(x => stageIds.contains(x.stageId)).map(_.taskMetrics.executorRunTime).sum
  }

  def stageRunTime(spark: SparkSession, stageIds: Set[Int]): Long = {
    spark.sparkContext.jobProgressListener.completedStages
      .filter(x => stageIds.contains(x.stageId)).map(x => x.completionTime.get - x.submissionTime.get).sum
  }

  def taskTimes(spark: SparkSession, stageIds: Seq[Int]): Seq[(Int, Seq[TaskInfo])] = {
    val taskInfos = stageIds.map{ stageId =>

      (stageId,
        spark.sparkContext.jobProgressListener.stageIdToData((stageId, 0))
          .taskData.values.map(_.taskInfo).toSeq)

    }

    taskInfos
  }

}
