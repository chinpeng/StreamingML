package evaluation

import clustering.clusters.KMeansPlusPlus
import generation.{StringToDense, StringToSparse}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.math.SparseVector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector


class ClusterEvaluator  extends Serializable{
  def computeSSQ(input: DataSet[String], centers:Array[SparseVector]): Double = {
    input
      .map(new StringToDense)
      .map(x => KMeansPlusPlus.findClosest(x, centers)._2)
        .map(x => (x, 1))
        .reduce((a, b) => (a._1+b._1, a._2+b._2)).map(x => x._1/x._2).collect().head
  }
}
