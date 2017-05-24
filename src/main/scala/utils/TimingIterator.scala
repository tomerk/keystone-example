package utils

/**
 * Created by tomerk11 on 5/24/17.
 *
 * Times the total time of the underlying iterator (in nanoseconds), then allows applying a function
 * on completion that uses it.
 * Built off of the completion iterator.
 */
abstract class TimingIterator[ +A, +I <: Iterator[A]](sub: I) extends Iterator[A] {
  // scalastyle:on

  protected var runtime: Long = 0l

  private[this] var completed = false
  def next(): A = {
    val startTime = System.nanoTime()
    val result = sub.next()
    runtime += System.nanoTime() - startTime

    result
  }
  def hasNext: Boolean = {
    val startTime = System.nanoTime()
    val r = sub.hasNext
    runtime += System.nanoTime() - startTime

    if (!r && !completed) {
      completed = true
      completion()
    }
    r
  }

  def completion(): Unit
}

object TimingIterator {
  def apply[A, I <: Iterator[A]](sub: I, completionFunction: Long => Unit) : TimingIterator[A, I] = {
    new TimingIterator[A, I](sub) {
      def completion(): Unit = completionFunction(runtime)
    }
  }
}
