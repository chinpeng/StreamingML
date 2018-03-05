package clustering

import clustering.clusters.{KMeansPlusPlus, LocalKMeans, MicroCluster, MicroClusters}
import generation.StringToDense
import math.DenseInstance
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.math.SparseVector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer
import scala.util.Random



class CluStream(
                 val q: Int,
                 val maxIterations: Int,
                 val initializationSteps: Int,
                 val k: Int,
                 val d: Int,
                 val m: Int,
                 val r: Double,
                 val h: Int,
                 val time: Int,
                 val output: String,
                 val method: Int,
                 val para: Int
               ) extends Serializable {

  var microClusters: MicroClusters = MicroClusters(Array[MicroCluster](), m, r, h)
  var clusters: Array[SparseVector] = _



  class UpdateReduceFunction extends ReduceFunction[(MicroClusters, Int)]{
    override def reduce(value1: (MicroClusters, Int), value2: (MicroClusters, Int)): (MicroClusters, Int) = {
      value2
    }
  }

  class UpdateMicroClusters extends ProcessAllWindowFunction[SparseVector, (MicroClusters, Int), TimeWindow]{
    private final val reducingStateGlobalDesc = new ReducingStateDescriptor[(MicroClusters, Int)](
      "globalMcs",
      new UpdateReduceFunction(),
      createTypeInformation[(MicroClusters, Int)])

    override def process(
                          context: Context,
                          elements: Iterable[SparseVector],
                          out: Collector[(MicroClusters, Int)]): Unit = {
      val reducingGlobalState = context.globalState.getReducingState(reducingStateGlobalDesc)

      if (reducingGlobalState.get() == null){
        reducingGlobalState.add((microClusters, 0))
      }

      var newMcs: (MicroClusters, Int) = reducingGlobalState.get()
      var count = 0
      for(e <- elements){
        newMcs = (newMcs._1.update(new DenseInstance(e.toDenseVector.data)), newMcs._2 + 1)
        count += 1
      }
      reducingGlobalState.add(newMcs)
      out.collect((reducingGlobalState.get()._1, count))
    }

  }

  /**
    * update local micro-clusters for each part
    */
  class UpdateMicroClustersDis extends ProcessWindowFunction[(SparseVector, Int), (MicroClusters, Int), Int, TimeWindow]{
    private final val reducingStateGlobalDisDesc = new ReducingStateDescriptor[(MicroClusters, Int)](
      "globalDisMcs",
      new UpdateReduceFunction(),
      createTypeInformation[(MicroClusters, Int)])

    override def process( key: Int,
                          context: Context,
                          elements: Iterable[(SparseVector, Int)],
                          out: Collector[(MicroClusters, Int)]): Unit = {
      val reducingGlobalState = context.globalState.getReducingState(reducingStateGlobalDisDesc)

      if (reducingGlobalState.get() == null){
        reducingGlobalState.add((microClusters, 0))
      }

      // get the state(micro-clusters)
      var newMcs: (MicroClusters, Int) = reducingGlobalState.get()
      var count = 0
      for(e <- elements){
        // update micro-clusters for every data in the window
        newMcs = (newMcs._1.update(new DenseInstance(e._1.toDenseVector.data)), newMcs._2 + 1)
        count += 1
      }
      // update the state
      reducingGlobalState.add(newMcs)
      out.collect((reducingGlobalState.get()._1, count))
    }

  }

  /**
    * serial update micro-clusters
    * @param input
    */
  def trainSerial(input: DataStream[SparseVector]): Unit ={
    input
      .timeWindowAll(Time.seconds(time))
      .process(new UpdateMicroClusters)
      .map(x => (trainOffCluster(x._1), x._2))
        .writeAsText(output)
      .setParallelism(1)
  }


  /**
    * parallel update micro-clusters
    * @param input
    */
  def trainDis(input: DataStream[SparseVector]): Unit ={

    input
      .map(x => {
        val ran = new Random
        (x, ran.nextInt(para))
      })
        .keyBy(_._2)
        .timeWindow(Time.seconds(time))
      .process(new UpdateMicroClustersDis)
      .timeWindowAll(Time.seconds(time))
        .apply(new MergeMicroClusters)
        .writeAsText(output)
      .setParallelism(1)

  }

  /**
    * merge the local micro-clusters to get global micro-clusters
    */
  class MergeMicroClusters extends AllWindowFunction[(MicroClusters, Int), (String, Int), TimeWindow] {

    def apply(window: TimeWindow, input: Iterable[(MicroClusters, Int)], out: Collector[(String, Int)]): Unit =  {

      val finalMcs = ArrayBuffer[MicroCluster]()
      var count = 0
      for (in <- input){
        finalMcs.++=(in._1.microclusters)
        count += in._2
      }
      val finalMcsArray = finalMcs.toArray
      out.collect((trainOff(finalMcsArray), count))
    }
  }

  /**
    * update micro-clusters parallel
    *
    * @param input
    */
  def trainDis1(input: DataStream[SparseVector]): Unit = {
    val nearest = input.map(x => (x, microClusters.findClosestKernel(DenseInstance(x.toDenseVector.data))))

    val points = nearest.filter(e => e._2._2 <= microClusters.computeRadius(e._2._1))
    val outliers = nearest.filter(e => e._2._2 > microClusters.computeRadius(e._2._1))

    points
      .keyBy(_._2._1)
      .timeWindow(Time.seconds(time))
      .aggregate(new AggregateMicroCluster)
      .timeWindowAll(Time.seconds(time))
      .aggregate(new GenNewMicroCluster)
      .map(x => {
        microClusters = x.mcs
        (trainOffCluster(), "process", x.count)
      }).print().setParallelism(1)

    outliers
      .timeWindowAll(Time.seconds(time))
      .fold {
        val createdMc = MicroClusters(Array.fill[MicroCluster](q)(MicroCluster(DenseInstance(Array.fill[Double](d)(0.0)),
          DenseInstance(Array.fill[Double](d)(0.0)), 0, 0, 0)), m, r, h)
        var x = 0
        for (clu <- microClusters.microclusters) {
          createdMc.microclusters(x) = clu
          x += 1
        }
        McAccumulator(createdMc, 0)
      }((acc: McAccumulator, in: (SparseVector, (Int, Double))) => {
        val timestamp = System.currentTimeMillis / 1000

        val newMc = acc.mcs.processOutliers(DenseInstance(in._1.toDenseVector.data), timestamp, h, m, r)
        McAccumulator(newMc, acc.count + 1)

      }).map(x => {
      microClusters = x.mcs
      (trainOffCluster(), "outlier", x.count)
    }).print().setParallelism(1)
  }


  class GenNewMicroCluster extends AggregateFunction[McAccumulator, McAccumulator, McAccumulator] {
    override def createAccumulator: McAccumulator = {
      val createdMc = MicroClusters(Array.fill[MicroCluster](q)(MicroCluster(DenseInstance(Array.fill[Double](d)(0.0)),
        DenseInstance(Array.fill[Double](d)(0.0)), 0, 0, 0)), m, r, h)
      var i = 0
      for (clu <- microClusters.microclusters) {
        createdMc.microclusters(i) = clu
        i += 1
      }
      McAccumulator(createdMc, 0)
    }

    override def add(in: McAccumulator, acc: McAccumulator): Unit = {
      var index = 0
      for (clu <- in.mcs.microclusters) {
        acc.mcs.microclusters(index) = in.mcs.microclusters(index).merge(acc.mcs.microclusters(index))
        index += 1
      }
      acc.count += in.count
    }

    override def merge(a: McAccumulator, b: McAccumulator): McAccumulator = {
      a.mcs = a.mcs.mergeTwoMicroClusters(b.mcs)
      a.count += b.count
      a
    }

    override def getResult(acc: McAccumulator): McAccumulator = {
      acc
    }
  }

  case class McAccumulator(var mcs: MicroClusters, var count: Int)

  class AggregateMicroCluster extends AggregateFunction[(SparseVector, (Int, Double)), McAccumulator, McAccumulator] {

    override def createAccumulator: McAccumulator = {
      McAccumulator(MicroClusters(Array.fill[MicroCluster](q)(MicroCluster(DenseInstance(Array.fill[Double](d)(0.0)),
        DenseInstance(Array.fill[Double](d)(0.0)), 0, 0, 0)), m, r, h), 0)
    }

    override def add(in: (SparseVector, (Int, Double)), acc: McAccumulator): Unit = {
      val timestamp = System.currentTimeMillis / 1000
      val idx = in._2._1
      val mc = acc.mcs.microclusters(idx)
      val sumInce = mc.sum.add(in._1)
      val sqSumInce = mc.sqSum.addSq(in._1)
      val timeSum = mc.timeSum + timestamp
      val sqTimeSum = mc.sqTimeSum + timestamp * timestamp
      val newMc = new MicroCluster(sumInce, sqSumInce, timeSum, sqTimeSum, mc.num + 1)

      acc.mcs.microclusters(idx) = newMc
      acc.count = acc.count + 1
    }

    override def merge(a: McAccumulator, b: McAccumulator): McAccumulator = {
      a.mcs = a.mcs.mergeTwoMicroClusters(b.mcs)
      a.count += b.count
      a
    }

    override def getResult(acc: McAccumulator): McAccumulator = {
      acc
    }

  }

  /**
    * build initial micro-clusters parallel
    *
    * @param events
    */
  def buildMicroModel(events: DataSet[SparseVector]): Unit = {
    val centers = KMeansPlusPlus.train(events,
      q, maxIterations, initializationSteps)
    val timestamp = System.currentTimeMillis / 1000
    microClusters = events.map(
      e => {
        val arr = Array.fill[Double](d)(0.0)
        val partMicroClusters = MicroClusters(Array.fill[MicroCluster]
          (q)(MicroCluster(new DenseInstance(arr),
          new DenseInstance(arr), 0, 0, 0)), m, r, h)
        val (idx, cost) = KMeansPlusPlus.findClosest(e, centers)
        val newMicorClusters = partMicroClusters.addToMicroCluster(idx, e, timestamp)
        newMicorClusters
      }
    ).reduce(
      (clu1, clu2) =>
        clu1.mergeTwoMicroClusters(clu2)
    ).collect().head

  }

  /**
    * train and get current cluster offline
    *
    * @return
    */
  def trainOffCluster(): String = {
    val points = ArrayBuffer[SparseVector]()
    val weights = ArrayBuffer[Double]()
    for (microCluster: MicroCluster <- microClusters.microclusters) {
      if (microCluster.num == 0) {
        points.+=(DenseInstance(Array.fill[Double](d)(0.0)).toSparseVector)
      }
      else {
        points.+=(microCluster.centroid.toSparseVector)
      }

      weights.+=(microCluster.num)
    }

    clusters = LocalKMeans.kMeansPlusPlus(0L, points.toArray, weights.toArray, k, 5)

    val ssq = microClusters.microclusters.map {
      point => {
        var sparsePointCentroid = DenseInstance(Array.fill[Double](d)(0.0)).toSparseVector
        if (point.num != 0) {
          sparsePointCentroid = point.centroid.toSparseVector
        }
        val (index, cost) = KMeansPlusPlus.findClosest(sparsePointCentroid, clusters)
        val center = clusters(index)
        center.data.map(x => x * x).sum * point.num +
          point.sqSum.reduce(_ + _) +
          point.sum.hadamard(center).map(-2 * _).reduce(_ + _)
      }
    }.sum/ weights.sum
    ssq.toString
  }

 def trainOff(mcs: Array[MicroCluster]): String = {
    val points = ArrayBuffer[SparseVector]()
    val weights = ArrayBuffer[Double]()
    val weightsCluster = ArrayBuffer[Double]()
    for (microCluster: MicroCluster <- mcs) {
      if (microCluster.num == 0) {
        points.+=(DenseInstance(Array.fill[Double](d)(0.0)).toSparseVector)
      }
      else {
        points.+=(microCluster.centroid.toSparseVector)
      }

      weights.+=(microCluster.timeSum)
      weightsCluster.+=(microCluster.num)

    }

    val clusters = LocalKMeans.kMeansPlusPlus(0L, points.toArray, weights.toArray, k, 5)

    val ssq = mcs.map {
      point => {
        var sparsePointCentroid = DenseInstance(Array.fill[Double](d)(0.0)).toSparseVector
        if (point.num != 0) {
          sparsePointCentroid = point.centroid.toSparseVector
        }
        val (index, cost) = KMeansPlusPlus.findClosest(sparsePointCentroid, clusters)
        val center = clusters(index)
        center.data.map(x => x * x).sum * point.num +
          point.sqSum.reduce(_ + _) +
          point.sum.hadamard(center).map(-2 * _).reduce(_ + _)
      }
    }.sum/ weightsCluster.sum
    ssq.toString
  }

  def trainOffCluster(mcs: MicroClusters): String = {
    val points = ArrayBuffer[SparseVector]()
    val weights = ArrayBuffer[Double]()
    for (microCluster: MicroCluster <- mcs.microclusters) {
      if (microCluster.num == 0) {
        points.+=(DenseInstance(Array.fill[Double](d)(0.0)).toSparseVector)
      }
      else {
        points.+=(microCluster.centroid.toSparseVector)
      }

      weights.+=(microCluster.num)
    }

    val clusters = LocalKMeans.kMeansPlusPlus(0L, points.toArray, weights.toArray, k, 5)

    val ssq = mcs.microclusters.map {
      point => {
        var sparsePointCentroid = DenseInstance(Array.fill[Double](d)(0.0)).toSparseVector
        if (point.num != 0) {
          sparsePointCentroid = point.centroid.toSparseVector
        }
        val (index, cost) = KMeansPlusPlus.findClosest(sparsePointCentroid, clusters)
        val center = clusters(index)
        center.data.map(x => x * x).sum * point.num +
          point.sqSum.reduce(_ + _) +
          point.sum.hadamard(center).map(-2 * _).reduce(_ + _)
      }
    }.sum/ weights.sum
    ssq.toString
  }

}

object CluStream extends Serializable {

  def run(
           initialData: DataSet[String],
           data: DataStream[String],
           q: Int,
           maxIterations: Int,
           initializationSteps: Int,
           k: Int,
           d: Int,
           m: Int,
           r: Double,
           h: Int,
           time: Int,
           output: String,
           method: Int,
           para: Int
         ): Unit = {
    val clu = new CluStream(q, maxIterations, initializationSteps, k, d, m, r, h, time, output, method, para)
    val initialData_Sparse = initialData.map(new StringToDense)
    val data_sparse = data.map(new StringToDense)

    // initial micro-clusters with initial dataSet
    clu.buildMicroModel(initialData_Sparse)

    if (method ==1){
      clu.trainDis(data_sparse)
    }
    else{
      clu.trainSerial(data_sparse)
    }

  }
}
