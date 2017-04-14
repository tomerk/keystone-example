package keystoneml.bandits

import org.apache.spark.bandit.{Action, BanditTrait}

/**
 * Created by tomerk11 on 4/4/17.
 */
class OracleBandit[A, B](oracle: A => Int, funcs: Seq[A => B]) extends BanditTrait[A, B] {
  override def apply(in: A): B = funcs(oracle(in)).apply(in)

  override def applyAndOutputReward(in: A): (B, Action) = {
    val arm = oracle(in)
    val startTime = System.nanoTime()
    val result = funcs(arm).apply(in)
    val endTime = System.nanoTime()

    // Intentionally provide -1 * elapsed time as the reward, so it's better to be faster
    val reward: Double = startTime - endTime

    (result, Action(arm, reward))
  }

  override def vectorizedApply(in: Seq[A]): Seq[B] = in.map(apply)
}