package evaluation

import java.io.Serializable

import org.apache.flink.ml.math.SparseVector
import org.apache.flink.streaming.api.scala.DataStream


/**
  * Abstract class which defines the operations needed to evaluate learners.
  */
abstract class Evaluator extends Serializable{

  /**
    * Process the result of a predicted stream of Examples and Doubles.
    *
    * @param input the input stream containing (Example,Double) tuples
    * @return a stream of String with the processed evaluation
    */
  def addResult(input: DataStream[(SparseVector, Double)]):  DataStream[String]

  /**
    * Get the evaluation result.
    *
    * @return a Double containing the evaluation result
    */
  def getResult(): Double
}
