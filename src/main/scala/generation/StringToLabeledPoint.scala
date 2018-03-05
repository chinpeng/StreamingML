package generation

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.ml.math.{DenseVector, SparseVector}


class StringToLabeledPoint extends MapFunction[String, (Int, DenseVector)] with Serializable {

  override def map(s: String): (Int, DenseVector) = {
    val elems = s.split(" ")
    val label = elems(0).toInt
    val arr = elems(1).split(",").map(x => x.toDouble)
    (label, DenseVector(arr))
  }
}