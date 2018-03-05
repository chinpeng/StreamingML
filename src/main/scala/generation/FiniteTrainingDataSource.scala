package generation

import java.io.File

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

import scala.io.Source
import scala.util.control._


class FiniteTrainingDataSource(rate: Long, path: String)
  extends SourceFunction[String] with Serializable {
  private val fixedRate = rate
  private val pathString = path
  private val oneSecond = 1000000000
  private var isRunning: Boolean = true

  override def run(scxt: SourceContext[String]): Unit = {
    val loop = new Breaks
    val s = Source.fromFile(new File(pathString)).getLines()
    while (isRunning) {
      var targetDone: Long = 0
      val startTimeNanos = System.nanoTime()
      if (!s.hasNext) {
        cancel()
      }
      loop.breakable {
        while (targetDone < fixedRate && isRunning && s.hasNext) {
          targetDone += 1
          scxt.collect(s.next())
          throttleNanos(fixedRate, startTimeNanos, targetDone)
          val timeNow = System.nanoTime()
          if (timeNow - startTimeNanos >= oneSecond) {
            loop.break()
          }
        }
      }

    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }

  private def throttleNanos(ratePerSecond: Long, startTimeNanos: Long, targetDone: Long) {
    // throttle the operations
    val targetPerMs: Double = ratePerSecond / 1000.0
    val targetOpsTickNs: Long = (1000000 / targetPerMs).toLong
    if (targetPerMs > 0) {
      // delay until next tick
      val deadline = startTimeNanos + targetDone * targetOpsTickNs
      sleepUntil(deadline)
    }
  }

  private def sleepUntil(deadline: Long) {
    while (System.nanoTime() < deadline) {
    }
  }
}
