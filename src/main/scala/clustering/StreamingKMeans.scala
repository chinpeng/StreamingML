package clustering

import java.io.File

import clustering.clusters.{KMeansPlusPlus, LocalKMeans}
import generation.{StringToDense, StringToSparse}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.ml.math.{BLAS, DenseVector, SparseVector}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

import scala.io.Source



class StreamingKMeans(
                       initialData: DataSet[String],
                       updateCount: Int,
                       decay: Double,
                       k: Int,
                       d: Int,
                       maxIterations: Int,
                       initializationSteps: Int,
                       time: Int
                     ) extends Serializable {

  private val kMeansPlusPlus = new KMeansPlusPlus(k, maxIterations, initializationSteps, 1e-4, 1000L)
  private var cluster = kMeansPlusPlus.chooseInitialCenters(initialData.map(new StringToDense))

  def train(data: DataStream[String]): Unit = {

    data
      .map(new StringToDense)
      .timeWindowAll(Time.seconds(time))
      .apply { (
                 window: TimeWindow,
                 events: Iterable[SparseVector],
                 out: Collector[(Double, Long)]) =>
        out.collect(update(events))
      }
      .map {
        x => printClusterMse(x._1, x._2)
      }
      .print().setParallelism(1)
  }


  def update(events: Iterable[SparseVector]): (Double, Long) = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val data: DataSet[SparseVector] = env.fromCollection(events)
    var converged = false
    var iteration = 0
    val arr = Array.fill[Double](d)(1.0)
    val centers = Array.fill[SparseVector](k)(DenseVector.apply(arr).toSparseVector)
    var index = 0
    for (center <- cluster) {
      centers.update(index, center)
      index += 1
    }
    // Execute iterations of Lloyd's algorithm until converged
    while (iteration < maxIterations && !converged) {
      // Find the sum and count of points mapping to each center

      val newCenters = data
        .mapWithBcVariable(data.getExecutionEnvironment.fromCollection(centers)) {
          (point, center) => {
            val (bestCenter, cost) = KMeansPlusPlus.findClosest(point, Array(center))
            //should be min (cost, preCost)
            (bestCenter, (point, 1))
          }
        }
        .groupBy(0).reduce((a, b) => {
        BLAS.axpy(1.0, b._2._1.toDenseVector, a._2._1.toDenseVector)
        (a._1, (a._2._1, a._2._2 + b._2._2))
      }).map(x => {
        BLAS.scal(1.0 / x._2._2, x._2._1)
        (x._1, x._2._1)
      }
      ).collect()

      // Update the cluster centers and costs
      converged = true
      newCenters.foreach { case (j, newCenter) =>
        if (converged && KMeansPlusPlus.fastSquaredDistance(newCenter, centers(j))
          > kMeansPlusPlus.getEpsilon * kMeansPlusPlus.getEpsilon) {
          converged = false
        }
        centers.update(j, newCenter)
      }

      iteration += 1
    }
    var i = 0
    for (center <- centers) {
      cluster.update(i, center)
      i += 1
    }
    //    cluster

    val cost = events
      .map(KMeansPlusPlus.findClosest(_, cluster))
      .map(_._2)
      .sum / events.toArray.length
    (cost, events.toArray.length)

  }

  def printClusterMse(cost: Double, count: Long): String = {
    cost.toString + "," + count.toString
  }

}

object StreamingKMeans extends Serializable {
  def run(
           initialData: DataSet[String],
           data: DataStream[String],
           updateCount: Int,
           decay: Double,
           k: Int,
           d: Int,
           maxIterations: Int,
           initializationSteps: Int,
           time: Int
         ): Unit = {
    val clu = new StreamingKMeans(initialData, updateCount, decay, k, d, maxIterations, initializationSteps, time)
    clu.train(data)

  }
}

