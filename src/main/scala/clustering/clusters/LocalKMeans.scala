package clustering.clusters

import org.apache.flink.ml.math.{DenseVector, SparseVector}
import org.apache.flink.ml.math.BLAS

import scala.util.Random

object LocalKMeans extends Serializable {
  def kMeansPlusPlus(
                      seed: Long,
                      points: Array[SparseVector],
                      weights: Array[Double],
                      k: Int,
                      maxIterations: Int
                    ): Array[SparseVector] = {
    val rand = new Random(seed)
    val dimensions = points(0).size
    val centers = new Array[SparseVector](k)

    // Initialize centers by sampling using the k-means++ procedure.
    centers(0) = pickWeighted(rand, points, weights)
    val costArray = points.map(KMeansPlusPlus.fastSquaredDistance(_, centers(0)))

    for (i <- 1 until k) {
      val sum = costArray.zip(weights).map(p => p._1 * p._2).sum
      val r = rand.nextDouble() * sum
      var cumulativeScore = 0.0
      var j = 0
      while (j < points.length && cumulativeScore < r) {
        cumulativeScore += weights(j) * costArray(j)
        j += 1
      }
      if (j == 0) {

        centers(i) = points(0)
      } else {
        centers(i) = points(j - 1)
      }

      // update costArray
      for (p <- points.indices) {
        costArray(p) = java.lang.Math.min(KMeansPlusPlus.fastSquaredDistance(points(p), centers(i)), costArray(p))
      }

    }

    // Run up to maxIterations iterations of Lloyd's algorithm
    val oldClosest = Array.fill(points.length)(-1)
    var iteration = 0
    var moved = true
    while (moved && iteration < maxIterations) {
      moved = false
      val counts = Array.fill(k)(0.0)
      val sums = Array.fill(k)(DenseVector.zeros(dimensions))
      var i = 0
      while (i < points.length) {
        val p = points(i)
        val index = KMeansPlusPlus.findClosest(p, centers)._1
        BLAS.axpy(weights(i), p, sums(index))
        counts(index) += weights(i)
        if (index != oldClosest(i)) {
          moved = true
          oldClosest(i) = index
        }
        i += 1
      }
      // Update centers
      var j = 0
      while (j < k) {
        if (counts(j) == 0.0) {
          // Assign center to a random point
          centers(j) = points(rand.nextInt(points.length))
        } else {
          BLAS.scal(1.0 / counts(j), sums(j))
          centers(j) = sums(j).toSparseVector
        }
        j += 1
      }
      iteration += 1
    }
    centers
  }

  def costAverage(points: Array[SparseVector],
                  centers: Array[SparseVector],
                  weights: Array[Double]): Double = {
    val cost = points
      .map(KMeansPlusPlus.findClosest(_, centers))
      .map(_._2)
      .zip(weights)
      .map(p => p._1 * p._2)
      .sum / weights.sum
    cost
  }

  private def pickWeighted[T](rand: Random, data: Array[T], weights: Array[Double]): T = {
    val r = rand.nextDouble() * weights.sum
    var i = 0
    var curWeight = 0.0
    while (i < data.length && curWeight < r) {
      curWeight += weights(i)
      i += 1
    }
    data(i - 1)
  }
}
