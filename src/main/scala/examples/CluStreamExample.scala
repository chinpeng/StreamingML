package examples

import clustering.CluStream
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark


class LinearTimestamp(val rate: Long) extends AssignerWithPunctuatedWatermarks[String] {
  private val serialVersionUID: Long = 1L

  private var counter: Long = 0L

  override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = {
    counter += rate
    counter
  }

  override def checkAndGetNextWatermark(lastElement: String, extractedTimestamp: Long): Watermark = {
    new Watermark(counter - 1)
  }

}
object CluStreamExample extends Serializable {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val envSet = ExecutionEnvironment.getExecutionEnvironment

    val params = ParameterTool.fromArgs(args)
    val interval = if (params.has("interval")) params.getInt("interval") else 10000
    env.getConfig.setLatencyTrackingInterval(interval)
    val path = if (params.has("train")) params.get("train") else "./data/kddcup.data_10_percent_corrected_train_normalized.csv"
    val pathInitial = if (params.has("initial")) params.get("initial") else "./data/kddcup.data_10_percent_corrected_train_normalized.csv"

    val maxIterations = if (params.has("maxIterations")) params.getInt("maxIterations") else 5
    val q: Int = if (params.has("q")) params.getInt("q") else 50
    val initializationSteps: Int = if (params.has("initializationSteps")) params.getInt("initializationSteps") else 5
    val k: Int = if (params.has("k")) params.getInt("k") else 5
    val d: Int = if (params.has("d")) params.getInt("d") else 34
    val m: Int = if (params.has("m")) params.getInt("m") else 100
    val r: Double = if (params.has("r")) params.getDouble("r") else 2.0
    val h: Int = if (params.has("h")) params.getInt("h") else 100000
    val time: Int = if (params.has("time")) params.getInt("time") else 1
    val rate: Long = if (params.has("rate")) params.getLong("rate") else 1L
    val output = if (params.has("output")) params.get("output") else "./data/output1"
    val jobName = if (params.has("jobName")) params.get("jobName") else "CluStream"
    val source: Int = if (params.has("source")) params.getInt("source") else 1
    val para: Int = if (params.has("para")) params.getInt("para") else 4

    // 1: parallel, else: serial
    val method: Int = if (params.has("method")) params.getInt("method") else 1

    val initialData = envSet.readTextFile(pathInitial)

    val trainingData = env
        .readTextFile(path)
        .setParallelism(para)
      .assignTimestampsAndWatermarks(new LinearTimestamp(rate))

      CluStream.run(initialData, trainingData,
        q, maxIterations,
        initializationSteps,
        k, d, m, r, h, time, output, method, para)

    env.execute(jobName)
  }
}
