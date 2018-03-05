package math

import org.apache.flink.ml.math.{DenseMatrix, DenseVector, Matrix, SparseMatrix, SparseVector, Vector}

case class DenseInstance(data: Array[Double]) extends Vector with Serializable {

  type T = DenseInstance

  /**
    * Number of elements in a vector
    *
    * @return the number of the elements in the vector
    */
  override def size: Int = {
    data.length
  }

  /**
    * Element wise access function
    *
    * @param index index of the accessed element
    * @return element at the given index
    */
  override def apply(index: Int): Double = {
    require(0 <= index && index < data.length, index + " not in [0, " + data.length + ")")
    data(index)
  }

  override def toString: String = {
    s"DenseInstance(${data.mkString(", ")})"
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case dense: DenseVector => data.length == dense.data.length && data.sameElements(dense.data)
      case _ => false
    }
  }

  override def hashCode: Int = {
    java.util.Arrays.hashCode(data)
  }

  /**
    * Copies the vector instance
    *
    * @return Copy of the vector instance
    */
  override def copy: DenseVector = {
    DenseVector(data.clone())
  }

  /** Updates the element at the given index with the provided value
    *
    * @param index Index whose value is updated.
    * @param value The value used to update the index.
    */
  override def update(index: Int, value: Double): Unit = {
    require(0 <= index && index < data.length, index + " not in [0, " + data.length + ")")

    data(index) = value
  }

  /** Returns the dot product of the recipient and the argument
    *
    * @param other a Vector
    * @return a scalar double of dot product
    */
  override def dot(other: Vector): Double = {
    require(size == other.size, "The size of vector must be equal.")

    other match {
      case SparseVector(_, otherIndices, otherData) =>
        otherIndices.zipWithIndex.map {
          case (idx, sparseIdx) => data(idx) * otherData(sparseIdx)
        }.sum
      case _ => (0 until size).map(i => data(i) * other(i)).sum
    }
  }

  /** Returns the outer product (a.k.a. Kronecker product) of `this`
    * with `other`. The result will given in [[org.apache.flink.ml.math.SparseMatrix]]
    * representation if `other` is sparse and as [[org.apache.flink.ml.math.DenseMatrix]] otherwise.
    *
    * @param other a Vector
    * @return the [[org.apache.flink.ml.math.Matrix]] which equals the outer product of `this`
    *         with `other.`
    */
  override def outer(other: Vector): Matrix = {
    val numRows = size
    val numCols = other.size

    other match {
      case sv: SparseVector =>
        val entries = for {
          i <- 0 until numRows
          (j, k) <- sv.indices.zipWithIndex
          value = this (i) * sv.data(k)
          if value != 0
        } yield (i, j, value)

        SparseMatrix.fromCOO(numRows, numCols, entries)
      case _ =>
        val values = for {
          i <- 0 until numRows
          j <- 0 until numCols
        } yield this (i) * other(j)

        DenseMatrix(numRows, numCols, values.toArray)
    }
  }

  /** Magnitude of a vector
    *
    * @return The length of the vector
    */
  override def magnitude: Double = {
    java.lang.Math.sqrt(data.map(x => x * x).sum)
  }

  /** Convert to a [[SparseVector]]
    *
    * @return Creates a SparseVector from the DenseVector
    */
  def toSparseVector: SparseVector = {
    val nonZero = (0 until size).zip(data).filter(_._2 != 0)

    if (nonZero.size == 0) {
      SparseVector.fromCOO(size, (0, 0.0))
    }
    else {
      SparseVector.fromCOO(size, nonZero)
    }
  }

  def add(x: Vector): DenseInstance = x match {

    case DenseInstance(f: Array[Double]) =>
      var newF: Array[Double] = Array.fill(data.length)(0.0)
      var k = 0
      while (k < data.length) {
        newF(k) = data(k) + f(k)
        k += 1
      }
      new DenseInstance(newF)

    case DenseVector(f: Array[Double]) =>
      var newF: Array[Double] = Array.fill(data.length)(0.0)
      var k = 0
      while (k < data.length) {
        newF(k) = data(k) + f(k)
        k += 1
      }
      new DenseInstance(newF)

    case SparseVector(xsize, xindices, f) =>
      var newF: Array[Double] = Array.fill(data.length)(0.0)
      var k = 0
      while (k < xindices.length) {
        newF(xindices(k)) = data(xindices(k)) + f(k)
        k += 1
      }
      new DenseInstance(newF)

    case _ =>
      new DenseInstance(data)
  }

  def addSq(x: Vector): DenseInstance = x match {

    case DenseInstance(f) =>
      var newF: Array[Double] = Array.fill(data.length)(0.0)
      var k = 0
      while (k < data.length) {
        newF(k) = data(k) + f(k) * f(k)
        k += 1
      }
      new DenseInstance(newF)

    case DenseVector(f) =>
      var newF: Array[Double] = Array.fill(data.length)(0.0)
      var k = 0
      while (k < data.length) {
        newF(k) = data(k) + f(k) * f(k)
        k += 1
      }
      new DenseInstance(newF)

    case SparseVector(xsize, xindices, f) =>
      var newF: Array[Double] = Array.fill(data.length)(0.0)
      var k = 0
      while (k < xindices.length) {
        newF(xindices(k)) = data(xindices(k)) + f(k) * f(k)
        k += 1
      }
      new DenseInstance(newF)

    case _ =>
      new DenseInstance(data)
  }

  def hadamard(x: Vector): DenseInstance = x match {
    case DenseInstance(f) =>
      var newF: Array[Double] = Array.fill(data.length)(0.0)
      var k = 0
      while (k < data.length) {
        newF(k) = data(k) * f(k)
        k += 1
      }
      new DenseInstance(newF)

    case DenseVector(f) =>
      var newF: Array[Double] = Array.fill(data.length)(0.0)
      var k = 0
      while (k < data.length) {
        newF(k) = data(k) * f(k)
        k += 1
      }
      new DenseInstance(newF)

    case SparseVector(xsize, xindices, f) =>
      var newF: Array[Double] = Array.fill(data.length)(0.0)
      var k = 0
      while (k < xindices.length) {
        newF(xindices(k)) = data(xindices(k)) * f(k)
        k += 1
      }
      new DenseInstance(newF)

    case _ =>
      new DenseInstance(data)
  }

  def multiplyAddSq(x: Vector, multi: Int): DenseInstance = x match {

    case DenseInstance(f) =>
      var newF: Array[Double] = Array.fill(data.length)(0.0)
      var k = 0
      while (k < data.length) {
        newF(k) = data(k) + multi * f(k) * f(k)
        k += 1
      }
      new DenseInstance(newF)

    case DenseVector(f) =>
      var newF: Array[Double] = Array.fill(data.length)(0.0)
      var k = 0
      while (k < data.length) {
        newF(k) = data(k) + multi * f(k) * f(k)
        k += 1
      }
      new DenseInstance(newF)

    case SparseVector(xsize, xindices, f) =>
      var newF: Array[Double] = Array.fill(data.length)(0.0)
      var k = 0
      while (k < xindices.length) {
        newF(xindices(k)) = data(xindices(k)) + multi * f(k) * f(k)
        k += 1
      }
      new DenseInstance(newF)

    case _ =>
      new DenseInstance(data)
  }

  def map(func: Double => Double): DenseInstance = {
    new DenseInstance(data.map { case x => func(x) })
  }

  def reduce(func: (Double, Double) => Double): Double = {
    data.reduce(func)
  }
}

object DenseInstance extends Serializable {

  def apply(values: Double*): DenseInstance = {
    new DenseInstance(values.toArray)
  }

  def apply(values: Array[Int]): DenseInstance = {
    new DenseInstance(values.map(_.toDouble))
  }

  def zeros(size: Int): DenseInstance = {
    init(size, 0.0)
  }

  def eye(size: Int): DenseInstance = {
    init(size, 1.0)
  }

  def init(size: Int, value: Double): DenseInstance = {
    new DenseInstance(Array.fill(size)(value))
  }
}
