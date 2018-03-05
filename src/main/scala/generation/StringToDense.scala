package generation

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.ml.math.{DenseVector, SparseVector}


class StringToDense extends MapFunction[String, SparseVector] with Serializable {

  override def map(s: String): SparseVector = {
    val arr = s.split(",").map(x => x.toDouble)
    DenseVector(arr).toSparseVector
  }
}