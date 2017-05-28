package keystoneml.bandits

import org.apache.spark.bandit.{Action, BanditTrait, DelayedFeedbackProvider}

/**
 * Created by tomerk11 on 4/4/17.
 */
class ConstantBandit[A, B](arm: Int, func: A => B) extends BanditTrait[A, B] {
  override def apply(in: A): B = func(in)

  override def applyAndOutputReward(in: A): (B, Action) = {
    val startTime = System.nanoTime()
    val result = func(in)
    val endTime = System.nanoTime()

    // Intentionally provide -1 * elapsed time as the reward, so it's better to be faster
    val reward: Double = startTime - endTime

    (result, Action(arm, reward))
  }

  override def vectorizedApply(in: Seq[A]): Seq[B] = in.map(func)

  override def applyAndDelayFeedback(in: A): (B, DelayedFeedbackProvider) = {
    val startTime = System.nanoTime()
    val result = func(in)
    val endTime = System.nanoTime()

    (result, new DelayedFeedbackProvider {
      override def provide(reward: Double): Unit = Unit

      override def getRuntime: Long = endTime - startTime
    })
  }

}
