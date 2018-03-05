package clustering.clusters

import org.apache.flink.api.scala._
import org.apache.flink.ml.math.{BLAS, SparseVector}
import org.apache.flink.util.XORShiftRandom

import collection.mutable._
import scala.collection.mutable.ArrayBuffer


class KMeansPlusPlus(
                      private var k: Int,
                      private var maxIterations: Int,
                      private var initializationSteps: Int,
                      private var epsilon: Double,
                      private var seed: Long) extends Serializable {

  def this() = this(2, 20, 2, 1e-4, 1000L)

  def getK: Int = k

  def setK(k: Int): this.type = {
    require(k > 0,
      s"Number of clusters must be positive but got ${k}")
    this.k = k
    this
  }

  def getMaxIterations: Int = maxIterations

  def setMaxIterations(maxIterations: Int): this.type = {
    require(maxIterations >= 0,
      s"Maximum of iterations must be nonnegative but got ${maxIterations}")
    this.maxIterations = maxIterations
    this
  }

  def getInitializationSteps: Int = initializationSteps

  def setInitializationSteps(initializationSteps: Int): this.type = {
    require(initializationSteps > 0,
      s"Number of initialization steps must be positive but got ${initializationSteps}")
    this.initializationSteps = initializationSteps
    this
  }

  def getEpsilon: Double = epsilon

  def setEpsilon(epsilon: Double): this.type = {
    require(epsilon >= 0,
      s"Distance threshold must be non-negative but got ${epsilon}")
    this.epsilon = epsilon
    this
  }

  def run(data: DataSet[SparseVector]): Array[SparseVector] = {

    var centers = chooseInitialCenters(data)
    var converged = false
    var iteration = 0

    // Execute iterations of Lloyd's algorithm until converged
    while (iteration < maxIterations && !converged) {
      // Find the sum and count of points mapping to each center
      val newCenters = data
        .mapWithBcVariable(data.getExecutionEnvironment.fromCollection(centers)) {
          (point, center) => {
            val (bestCenter, cost) = KMeansPlusPlus.findClosest(point, Array(center))
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
        if (converged && KMeansPlusPlus.fastSquaredDistance(newCenter, centers(j)) > epsilon * epsilon) {
          converged = false
        }
        centers(j) = newCenter
      }

      iteration += 1
    }
    centers
  }

  def chooseInitialCenters(data: DataSet[SparseVector]): Array[SparseVector] = {
    val seed = new XORShiftRandom(this.seed).nextInt()
    // sample operator for Dataset: flink-1901
    var bcNewCenters: DataSet[SparseVector] = utils.DataSetUtils(data).sampleWithSize(false, 1, seed)
    var centers: ArrayBuffer[SparseVector] = ArrayBuffer[SparseVector]()
    centers.++=(bcNewCenters.collect)

    var step = 0
    while (step < initializationSteps) {
      val newCost = data.mapWithBcVariable(bcNewCenters) {
        (point, center) => (point, KMeansPlusPlus.findClosest(point, Array(center))._2)
      }
      val sumCosts = newCost.reduce((a, b) => (a._1, a._2 + b._2)).collect()
//      sumCosts.foreach(x => print(x))
      val sumCost = sumCosts(0)._2

      bcNewCenters = newCost.map {
        x =>
          val rand = new XORShiftRandom().nextDouble()
          (x._1, x._2, rand)
      }.filter(x => x._3 < 2.0 * x._2 * k / sumCost || x._2 == 0.0)
        .map(_._1)
      centers.++=(bcNewCenters.collect())
      step += 1
    }

    val distinctCenters = centers.distinct
    val countSeq = data
      .map {
        x =>
          val bestCenter = KMeansPlusPlus.findClosest(x, distinctCenters.toArray)._1
          bestCenter
      }
      .map(x => (x, 1L))
      .groupBy(0)
      .sum(1)

    val countMap = Map[Int, Long]()

    for (x <- countSeq.collect()) {
      countMap += (x._1 -> x._2)
    }
    val w = distinctCenters.indices.map(countMap.getOrElse(_, 0L).toDouble).toArray
    LocalKMeans.kMeansPlusPlus(0, distinctCenters.toArray, w, k, 30)

  }
}

object KMeansPlusPlus extends Serializable {

  def train(
             data: DataSet[SparseVector],
             k: Int,
             maxIterations: Int,
             initializationSteps: Int
           ): Array[SparseVector] = {
    new KMeansPlusPlus(k, maxIterations, initializationSteps, 1e-6, 1000L).run(data)
  }


  /**
    * @param point   each data point
    * @param centers Array of center got from broadcast variable
    * @return (bestIndex, bestDistance)
    *         bestIndex: index of closest center for the point in the centerArray
    *         bestDistance: distance between the point and the closest center
    **/
  def findClosest(point: SparseVector, centers: Array[SparseVector]): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    for (center: SparseVector <- centers) {
      var lowerBoundOfSqDist = new VectorWith2Norm(center).norm2 - new VectorWith2Norm(point).norm2()
      lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
      if (lowerBoundOfSqDist < bestDistance) {
        val distance: Double = fastSquaredDistance(center, point)
        if (distance < bestDistance) {
          bestDistance = distance
          bestIndex = i
        }
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }

  def fastSquaredDistance(v1: SparseVector, v2: SparseVector): Double = {
    val n = v1.size
    require(v2.size == n)
    val norm1 = new VectorWith2Norm(v1).norm2()
    val norm2 = new VectorWith2Norm(v2).norm2()
    require(norm1 >= 0.0 && norm2 >= 0.0)
    val sumSquaredNorm = norm1 * norm1 + norm2 * norm2
    val normDiff = norm1 - norm2
    var sqDist = 0.0
    /*
     * The relative error is
     * <pre>
     * EPSILON * ( \|a\|_2^2 + \|b\\_2^2 + 2 |a^T b|) / ( \|a - b\|_2^2 ),
     * </pre>
     * which is bounded by
     * <pre>
     * 2.0 * EPSILON * ( \|a\|_2^2 + \|b\|_2^2 ) / ( (\|a\|_2 - \|b\|_2)^2 ).
     * </pre>
     * The bound doesn't need the inner product, so we can use it as a sufficient condition to
     * check quickly whether the inner product approach is accurate.
     */
    val EPSILON = {
      var eps = 1.0
      while ((1.0 + (eps / 2.0)) != 1.0) {
        eps /= 2.0
      }
      eps
    }
    val precision = 1e-6

    val precisionBound1 = 2.0 * EPSILON * sumSquaredNorm / (normDiff * normDiff + EPSILON)
    if (precisionBound1 < precision) {
      sqDist = sumSquaredNorm - 2.0 * v1.dot(v2)
    } else if (v1.isInstanceOf[SparseVector] || v2.isInstanceOf[SparseVector]) {
      val dotValue = v1.dot(v2)
      sqDist = java.lang.Math.max(sumSquaredNorm - 2.0 * dotValue, 0.0)
      val precisionBound2 = EPSILON * (sumSquaredNorm + 2.0 * java.lang.Math.abs(dotValue)) /
        (sqDist + EPSILON)
      if (precisionBound2 > precision) {
        sqDist = sqdist(v1, v2)
      }
    } else {
      sqDist = sqdist(v1, v2)
    }
    sqDist
  }

  private def sqdist(v1: SparseVector, v2: SparseVector): Double = {
    require(v1.size == v2.size, s"Vector dimensions do not match: Dim(v1)=${v1.size} and Dim(v2)" +
      s"=${v2.size}.")
    var squaredDistance = 0.0

    val v1Values = v1.data
    val v1Indices = v1.indices
    val v2Values = v2.data
    val v2Indices = v2.indices
    val nnzv1 = v1Indices.length
    val nnzv2 = v2Indices.length

    var kv1 = 0
    var kv2 = 0
    while (kv1 < nnzv1 || kv2 < nnzv2) {
      var score = 0.0

      if (kv2 >= nnzv2 || (kv1 < nnzv1 && v1Indices(kv1) < v2Indices(kv2))) {
        score = v1Values(kv1)
        kv1 += 1
      } else if (kv1 >= nnzv1 || (kv2 < nnzv2 && v2Indices(kv2) < v1Indices(kv1))) {
        score = v2Values(kv2)
        kv2 += 1
      } else {
        score = v1Values(kv1) - v2Values(kv2)
        kv1 += 1
        kv2 += 1
      }
      squaredDistance += score * score
    }

    squaredDistance
  }

}

class VectorWith2Norm(val vector: SparseVector) extends Serializable {
  def norm2(): Double = {
    val value = vector.data
    val indices = vector.indices
    var i: Int = 0
    var norm: Double = 0.0
    while (i < vector.indices.length) {
      norm += value(i) * value(i)
      i += 1
    }
    java.lang.Math.sqrt(norm)
  }
}


