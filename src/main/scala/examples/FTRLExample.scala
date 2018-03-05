package examples

import regression.FTRL
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object FTRLExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val hostname: String = "localhost"
    val port: Int = 3000

    val trainingData = env.socketTextStream(hostname, port, '\n')

    val model = FTRL.train(trainingData)

    model.print().setParallelism(1)
    env.execute("model train")
  }
}
