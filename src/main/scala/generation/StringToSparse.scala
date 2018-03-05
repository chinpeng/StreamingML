package generation

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.ml.math.SparseVector


class StringToSparse extends MapFunction[String, SparseVector] with Serializable {

  override def map(s: String): SparseVector = {
    val arr = s.split(",")
    val size = arr.size
    val tuples = arr.map(x => {
      val elems = x.split(":")
      val index = elems(0).toInt
      val value = elems(1).toDouble
      (index, value)
    })
    SparseVector.fromCOO(size, tuples)
  }
}
