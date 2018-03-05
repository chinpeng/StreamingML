package examples

import clustering.{CluStream, StreamingKMeans}
import evaluation.ClusterEvaluator
import generation.FiniteTrainingDataSource
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.ml.math.{DenseVector, SparseVector}
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction.WatchType
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._


object StreamingKMeansExample extends Serializable {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val envSet = ExecutionEnvironment.getExecutionEnvironment


    val params = ParameterTool.fromArgs(args)
    //      val parallelism = if (params.has("parallelism")) params.getInt("parallelism") else 1
    //      env.setParallelism(parallelism)
    val interval = if (params.has("interval")) params.getInt("interval") else 10000
    env.getConfig.setLatencyTrackingInterval(interval)
    val path = if (params.has("train")) params.get("train") else "./data/orig"
    val pathInitial = if (params.has("initial")) params.get("initial") else "./data/orig"

    val maxIterations = if (params.has("maxIterations")) params.getInt("maxIterations") else 5
    val initializationSteps: Int = if (params.has("initializationSteps")) params.getInt("initializationSteps") else 5
    val k: Int = if (params.has("k")) params.getInt("k") else 10
    val d: Int = if (params.has("d")) params.getInt("d") else 3
    val time: Int = if (params.has("time")) params.getInt("time") else 10
    val rate: Long = if (params.has("rate")) params.getInt("rate") else 10

    val decay: Double = 0.5
    val updateCount: Int = 1000


    val initialData = envSet.readTextFile(pathInitial)
    val trainingData = env.addSource(new FiniteTrainingDataSource(rate, path))

    StreamingKMeans.run(initialData, trainingData,
      updateCount, decay, k, d, maxIterations,
      initializationSteps, time
    )


    env.execute("model train")
  }
}