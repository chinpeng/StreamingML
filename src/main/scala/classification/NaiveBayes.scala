package classification

import generation.StringToLabeledPoint
import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable


class NaiveBayes(val time: Int, val classes: Int, val d : Int) extends Serializable {
  type T = mutable.HashMap[Double, Long]
  // input: (label, DenseVector)
  def update(input: DataStream[(Int, DenseVector)]): DataStream[Array[T]] = {
    input.map(x => {
      (x._1, x._2.toSparseVector)
    }).flatMap(x => {
      val data = x._2.data
      val index = x._2.indices
      data.zip(index).map(elem => (x._1, elem)) // label, feature_value, feature_index
    }).keyBy(_._2._2).map(new MapStatics)
  }

  def update2(input: DataStream[(Int, DenseVector)]): DataStream[Array[Array[T]]]={
    input.keyBy(_._1)
      .timeWindow(Time.seconds(time))
      .process(new ProcessStatics)
      .timeWindowAll(Time.seconds(time))
      .apply(new GlobalStatics)
  }

  class GlobalStatics extends AllWindowFunction[Array[T], Array[Array[T]], TimeWindow] {

    def apply(window: TimeWindow, input: Iterable[Array[T]], out: Collector[Array[Array[T]]]): Unit =  {
      out.collect(input.toArray)
    }
  }

  class ReduceStaticsFunction extends ReduceFunction[Array[T]]{
    override def reduce(value1: Array[T], value2:Array[T]): Array[T] = {
      value2
    }
  }
  class ProcessStatics extends ProcessWindowFunction[(Int, DenseVector), Array[T], Int, TimeWindow]{
    private final val reducingStateGlobalDesc = new ReducingStateDescriptor[Array[T]](
      "globalDisMcs",
      new ReduceStaticsFunction(),
      createTypeInformation[Array[T]])

    override def process(key: Int,
                         context: Context,
                         elems: Iterable[(Int, DenseVector)],
                         out: Collector[ Array[T]]): Unit = {
      val reducingState = context.globalState.getReducingState(reducingStateGlobalDesc)

      val d = elems.head._2.data.length
      if (reducingState.get() == null){
          // initialize statics
        reducingState.add(Array.fill[T](d)(mutable.HashMap[Double, Long]()))
      }
      var keyedStatics = reducingState.get()
      for(e<-elems){
        val arr = e._2.data
        var index = 0
        for(a<-arr){
          val count = keyedStatics(index).getOrElse(a, 0L)+1L
          keyedStatics(index).update(a, count)
          keyedStatics.update(index, keyedStatics(index))
          index += 1
        }
      }
      reducingState.add(keyedStatics)
      out.collect(reducingState.get())
    }
  }


  // test data: id, label, features
  def run(model: DataStream[Array[T]], test:DataStream[(Int, Int, DenseVector)]): Unit = {
    val input_test = test.map(x => {
      (x._1, x._2, x._3.toSparseVector)
    }).flatMap(x => {
      val data = x._3.data
      val index = x._3.indices
      data.zip(index).map(elem => (x._1, x._2, elem)) // id, label, (value, feature_index)
    })
    input_test.connect(model)
      .map(new PredictorFeature)
  }

  class PredictorFeature extends CoMapFunction[(Int, Int, (Double, Int)), Array[T], Double]{
    private var partialStatics = Array.fill[T](classes)(mutable.HashMap[Double, Long]())
    override def map1(value: (Int, Int, (Double, Int))): Double = {
      0.0
    }

    override def map2(value: Array[T]): Double = {
      partialStatics = value
      -1.0
    }
  }

  def run2(model: DataStream[Array[Array[T]]], testData: DataStream[(Int, DenseVector)]): DataStream[Double] ={
    testData.connect(model)
      .map(new Predictor)
      .filter(x => x>0)
  }


  class MapStatics extends RichMapFunction[(Int, (Double, Int)), Array[T]] {
    private var statics: ValueState[Array[T]] = _

    override def map(in: (Int, (Double, Int))): Array[T] = {

      var static = statics.value
      if(static == null){
        static =  Array.fill[T](classes)(mutable.HashMap[Double, Long]())
      }
      val static_class = static(in._1)
      val count: Long = static_class.getOrElse(in._2._1, 0L)
      static_class.update(in._2._1, count + 1L)

      static.update(in._1, static_class)
      statics.update(static)
      statics.value

    }

    override def open(parameters: Configuration): Unit = {
      statics = getRuntimeContext.getState(
        new ValueStateDescriptor[Array[T]]("statics", createTypeInformation[Array[T]])
      )
    }
  }

  class Predictor extends CoMapFunction[(Int,DenseVector), Array[Array[T]], Double]{

    private var partialModel = Array.fill[Array[T]](classes)(Array.fill[T](d)(mutable.HashMap[Double, Long]()))

    override def map1(value: (Int,DenseVector)): Double = {
      var c = 0
      var index = 0
      var prob = 1.0
      val arr = value._2.data
      while(c < classes){
        while(index<arr.length){
          prob = prob * partialModel(c)(index).getOrElse(arr(index), 1L)/partialModel(c).reduce((a, b)=> a.values.sum+b.values.sum)
          index += 1
        }
        c += 1
      }
      prob
    }

    override def map2(value: Array[Array[T]]): Double = {
      partialModel = value
      -1.0
    }
  }

}

object NaiveBayes extends Serializable {
  def run(input: DataStream[String],
          time: Int,
          classes: Int,
          d: Int,
          test: DataStream[String]
         ): Unit = {
    val nb = new NaiveBayes(time, classes, d)
    val model = nb.update2(input.map(new StringToLabeledPoint))
//        .writeAsText("./data/outNB")
    nb.run2(model, test.map(new StringToLabeledPoint)).writeAsText("./data/outNB")
  }
}

