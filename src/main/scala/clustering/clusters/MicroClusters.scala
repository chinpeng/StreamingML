package clustering.clusters

import math.DenseInstance
import org.apache.flink.ml.math.SparseVector
import org.apache.flink.ml.metrics.distances.EuclideanDistanceMetric

import scala.collection.mutable.ArrayBuffer


case class MicroClusters(
                          var microclusters: Array[MicroCluster],
                          var mOption: Int,
                          var radiusOption: Double,
                          var horizonOption: Int) extends Serializable {

  /**
    * Find the closest kernel to the instance
    *
    * @param point
    * @return
    */
  def findClosestKernel(point: DenseInstance): (Int, Double) = {
    val clTuple = microclusters.foldLeft((0, Double.MaxValue, 0))((cl, mcl) => {
      val dist = EuclideanDistanceMetric.apply.distance(point, mcl.centroid)
      if (dist < cl._2) (cl._3, dist, cl._3 + 1)
      else (cl._1, cl._2, cl._3 + 1)
    })
    val closest = clTuple._1
    val distance = clTuple._2
    (closest, distance)
  }


  /**
    * compute radius of the micro-clusters
    *
    * @param idx
    * @return
    */
  def computeRadius(idx: Int): Double = {
    radiusOption * microclusters(idx).rmse
  }

  /**
    * process the outliers which can not be absorbed in current micro-clusters
    *
    * @param change    new point
    * @param timestamp current time
    * @param h         the time threshold to delete the oldest micro-cluster
    * @param m         number of latest point to compute the oldest timestamp
    * @param r         radius to absorb in a current micro-cluster
    * @return
    */
  def processOutliers(change: DenseInstance, timestamp: Long, h: Int, m: Int, r: Double): MicroClusters = {
    val mc = MicroCluster(change.map(x => x), change.map(x => x * x),
      timestamp, timestamp * timestamp, 1)
    //Find whether an expired microcluster exists
    val threshold = timestamp - h
    var i: Int = 0
    var found: Boolean = false
    var tmc: Int = 0
    while (i < microclusters.length && !found) {
      if (microclusters(i).threshold(m) < threshold) {
        found = true
        tmc = i
      }
      i += 1
    }
    if (found) {
      val newMc = removeMicroCluster(tmc).appendMicroCluster(mc)
      newMc
    }
    else {
      //find the two closest microclusters
      var sm: Int = 0
      var tm: Int = 0
      var dist: Double = Double.MaxValue
      var i: Int = 0
      var j: Int = 0
      while (i < microclusters.length) {
        j = i + 1
        while (j < microclusters.length) {
          val dst = this.distance(i, j)
          if (dst <= dist) {
            sm = i
            tm = j
            dist = dst
          }
          j += 1
        }
        i += 1
      }
      if (sm != tm) {
        val newMc = mergeMicroClusters(sm, tm).appendMicroCluster(mc)
        newMc
      }
      else {
        val newMc = MicroClusters(microclusters, m, r, h)
        newMc
      }
    }
  }

  /**
    * update the micro-clusters
    *
    * @param change new point
    * @return
    */
  def update(change: DenseInstance): MicroClusters = {
    val timestamp = System.currentTimeMillis / 1000
    //Find the closest kernel to the instance
    val cloDis = findClosestKernel(change)
    val closest = cloDis._1
    val distance = cloDis._2
    //Compute the radius of the closest microcluster
    val radius = computeRadius(closest)
    if (distance <= radius) {
      val recent = addToMicroCluster(closest, change.toSparseVector, timestamp)
      recent
    }
    else {
      val recent = processOutliers(change, timestamp, horizonOption, mOption, radiusOption)
      recent
    }
  }

  /**
    * merge two microClusters with same indexes
    **/
  def mergeTwoMicroClusters(other: MicroClusters): MicroClusters = {
    var idx = 0
    var arrayMicroCluster = ArrayBuffer[MicroCluster]()
    for (clu <- microclusters) {
      arrayMicroCluster.+=(microclusters(idx).merge(other.microclusters(idx)))
      idx += 1
    }
    MicroClusters(arrayMicroCluster.toArray, mOption, radiusOption, horizonOption)

  }

  /**
    * add a new point to the specified place of the micro-clusters
    *
    * @param index     specified place
    * @param change    new point
    * @param timestamp current time
    * @return
    */
  def addToMicroCluster(index: Int, change: SparseVector, timestamp: Long): MicroClusters = {
    MicroClusters(microclusters.updated(index,
      microclusters(index).insert(change, timestamp)), mOption, radiusOption, horizonOption)
  }

  /**
    * append a new micro-cluster to the current micro-clusters
    *
    * @param mc
    * @return
    */
  def appendMicroCluster(mc: MicroCluster): MicroClusters = {
    MicroClusters(microclusters :+ mc, mOption, radiusOption, horizonOption)
  }

  /**
    * remove a specified micro-cluster from the current micro-clusters
    *
    * @param index specified index
    * @return
    */
  def removeMicroCluster(index: Int): MicroClusters = {
    MicroClusters(microclusters.take(index) ++ microclusters.drop(index + 1), mOption, radiusOption, horizonOption)
  }

  /**
    * merge two specified micro-clusters to one micro-cluster
    *
    * @param target
    * @param source
    * @return
    */
  def mergeMicroClusters(target: Int, source: Int): MicroClusters = {
    if (target != source) {
      val mergedMicroClusters = microclusters.updated(target, microclusters(target).merge(microclusters(source)))
      MicroClusters(mergedMicroClusters.take(source) ++ mergedMicroClusters.drop(source + 1), mOption, radiusOption, horizonOption)
    }
    else
      this
  }

  /**
    * compute the euclidean distance between two micro-clusters
    *
    * @param source
    * @param target
    * @return
    */
  def distance(source: Int, target: Int): Double =
    EuclideanDistanceMetric.apply.distance(microclusters(source).centroid, microclusters(target).centroid)
}
