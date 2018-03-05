package examples

import evaluation.ClusterEvaluator
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.ml.math.SparseVector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.mutable.ArrayBuffer


object ComputeSSQExample extends Serializable {


  def main(args: Array[String]): Unit = {
    val envSet = ExecutionEnvironment.getExecutionEnvironment
    val pathTest = "./data/kddcup.data_10_percent_corrected_gen_test.csv"
    val testData = envSet.readTextFile(pathTest)


    val modelString = envSet.readTextFile("./data/modelIncre").collect().toList
    var modelData = ""
    for (s <- modelString) {
      modelData = modelData.concat(s)
    }
    val modelArray: ArrayBuffer[SparseVector] = ArrayBuffer[SparseVector]()
    val modelTuple = modelData.split("n")
    for (clu <- modelTuple) {
      if (clu.size != 0) {
        val indices_data = clu.split(":")
        val incides = indices_data(0).split(", ").map(_.toInt)
        val data = indices_data(1).split(", ").map(_.toDouble)
        val cluster = SparseVector.apply(141, incides, data)
        modelArray.+=(cluster)
      }
    }
    val model = modelArray.toArray
    val evaluator = new ClusterEvaluator
    val cost = evaluator.computeSSQ(testData, model)
    println("cost: " + cost)
  }
}