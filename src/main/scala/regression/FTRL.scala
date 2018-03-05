package regression

import java.util

import regression.FTRL._
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
  *    - finally output: latest updated dimensions
  *    - todo: parameter lamda1/lamda2/alpha/beta/dim/timewindow should receive user input
  * test
  */
class FTRL extends Serializable {

  def run(trainingData: DataStream[String]): DataStream[String] = {
    val data: DataStream[Data] = trainingData
      .map(s => {
        var wTx = 0.0
        val splits = s.split(" ") // splits(0): label
        for (elem <- splits(1).split(',')) {
          wTx = wTx + elem.split(':')(1).toDouble * w.get(elem.split(':')(0).toInt)
        }
        (splits(0).toInt, splits(1), wTx)
      }
      )
      .flatMap(x => {
        x._2.split(',').map((x._1, _, x._3))
      }).map(x => {
      val splits = x._2.split(':')
      Data(splits(0).toInt, splits(1).toDouble, x._1, x._3)
    })

    val model: DataStream[String] = data
      .filter(x => x.index < dim && x.index >= 0)
      .keyBy(_.index)
      .timeWindow(Time.seconds(10))
      .apply { (
                 key: Int,
                 window: TimeWindow,
                 events: Iterable[Data],
                 out: Collector[String]) =>
        out.collect(buildPartialModel(key, events).toString)
      }
    model
  }

  def buildPartialModel(key: Int, value: Iterable[Data]): Params = {

    val iniW = w.get(key)
    var postW = iniW
    var prob = 0.0
    var zi = z(key)
    var ni = n(key)

    for (e <- value) {
      val x = predict(zi, ni, key, e, postW, iniW)
      postW = x._1
      prob = x._2
      val y = update(key, prob, postW, e, zi, ni)
      zi = y._1
      ni = y._2
    }

    w.put(key, postW)
    z(key) = zi
    n(key) = ni
    Params(key, postW)
  }

  def predict(zi: Double, ni: Double, key: Int, e: Data, postW: Double, iniW: Double): (Double, Double) = {
    var post = postW
    val sgn = {
      if (zi < 0) {
        -1
      }
      else {
        1
      }
    }
    if (sgn * zi <= lamda1) {
      post = 0
    }
    else {
      post = (sgn * lamda1 - zi) / (lamda2 + (beta + Math.sqrt(ni)) / alpha)
    }
    val wTx = e.wTx - iniW * e.value + post * e.value
    (post, 1 / (1 + Math.exp(-Math.max(Math.min(wTx, 35), -35))))
  }

  def update(key: Int, prob: Double, w: Double, e: Data, zi: Double, ni: Double): (Double, Double) = {
    var zii = zi
    var nii = ni
    val ans = {
      if (e.label > 0) {
        1
      }
      else {
        0
      }

    }
    val g: Double = (prob - ans) * e.value
    val sigma: Double = (Math.sqrt(ni + g * g) - Math.sqrt(ni)) / alpha
    zii = zii + g - sigma * w
    nii = nii + g * g
    (zii, nii)
  }

  case class Data(index: Int, value: Double, label: Double, wTx: Double)

  case class Params(i: Int, w: Double) {
    override def toString: String = s"weight of $i th feature is $w"
  }

}

object FTRL {

  var w = new util.HashMap[Int, Double]()
  val lamda1 = 0.1
  val lamda2 = 0.3
  val alpha = 0.03
  val beta = 1
  val dim = 100

  var z: Array[Double] = new Array[Double](dim)
  var n: Array[Double] = new Array[Double](dim)

  def train(trainingData: DataStream[String]): DataStream[String] = {
    new FTRL().run(trainingData)
  }
}


