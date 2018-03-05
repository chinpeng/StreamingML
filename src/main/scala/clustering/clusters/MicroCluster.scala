package clustering.clusters

import math.DenseInstance
import org.apache.flink.ml.math.{DenseVector, SparseVector}

case class MicroCluster(sum: DenseInstance,
                        sqSum: DenseInstance,
                        timeSum: Long,
                        sqTimeSum: Long,
                        num: Int) extends Serializable {

  /**
    * Insert a new data into a microCluster
    *
    * @param data data inserted into the micro-cluster
    * @param time timeStamp of the data
    * @return
    */
  def insert(data: SparseVector, time: Long): MicroCluster = {
    MicroCluster(sum.add(data),
      sqSum.addSq(data),
      timeSum + time,
      sqTimeSum + time * time,
      num + 1)
  }

  /**
    * Merge two same idx microCluster
    *
    * @param other the micro-cluster to be merged
    * @return
    */
  def merge(other: MicroCluster): MicroCluster = {
    MicroCluster(sum.add(other.sum), sqSum.add(other.sqSum),
      other.timeSum + timeSum, other.sqTimeSum + sqTimeSum, other.num + num)
  }

  /**
    * Compute the centroid of a micro-cluster
    *
    * @return
    */
  def centroid: DenseVector = {
    if (num > 1) {
      val nnz = sum.data.length
      val newSum = DenseInstance(Array.fill[Double](nnz)(0.0))
      var k = 0
      while (k < nnz) {
        newSum(k) = sum(k) / num.toDouble
        k += 1
      }
      DenseVector(newSum.data)
    }
    else {
      DenseVector(sum.data)
    }

  }

  /**
    * Compute the rmse of the micro-cluster
    *
    * @return
    */
  def rmse: Double = {
    if (num > 1) {
      val centr = this.centroid
      var count = 0
      var middleRes = sqSum
      middleRes = middleRes.multiplyAddSq(centr, num)
      Math.sqrt(middleRes.add(sum.hadamard(centr).
        map(-2 * _)).reduce(_ + _) / num)
    }
    else {
      0.0
    }
  }

  /**
    * Compute the mse of the micro-cluster
    *
    * @return
    */
  def mse: Double = {
    if (num <= 1)
      0.0
    else {
      val centr = this.centroid
      var middleRes = sqSum
      middleRes = middleRes.multiplyAddSq(centr, num)
      middleRes.add(sum.hadamard(centr).
        map(-2 * _)).reduce(_ + _) / num
    }
  }

  /**
    * Compute the threshold timestamp of the micro-cluster.
    *
    * @param m latest m points to compute threshold timestamp
    * @return the threshold timestamp
    */
  def threshold(m: Int): Double = {
    if (num <= 1)
      Double.MaxValue
    else {
      val mu = timeSum / num
      if (num < 2 * m)
        mu
      else {
        val z = 2 * m / num
        val sd = sqTimeSum / num - mu * mu
        mu + sd * java.lang.Math.sqrt(2) * inverr(2 * z - 1)
      }
    }

  }

  /**
    * Compute the inverse error functions, for computing the timestamp threshold
    *
    * @param x
    * @return
    */
  def inverr(x: Double): Double = {
    val pi = 3.14159
    val a = 0.140012
    val sgn = if (x < 0) -1 else 1
    val lg = java.lang.Math.log(1 - x * x)
    val fr = 2 / (pi * a) + lg / 2
    sgn * java.lang.Math.sqrt(java.lang.Math.sqrt(fr * fr - lg / a) - fr)
  }

}
