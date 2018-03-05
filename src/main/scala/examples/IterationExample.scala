package examples

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object IterationExample {

  class FunctionModel extends CoMapFunction[Long, Long, Long]{
    private var m = 0L
    override def map1(value: Long): Long = {
      m = value
      -1L
    }

    override def map2(value: Long): Long = {
      m += value
      m
    }
  }
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val model: DataStream[Long] = env.generateSequence(0, 1)

    val data: DataStream[Long] = env.generateSequence(0, 100)

//    val iterationStream = iter.iterate()
//      }
//    )
//val someIntegers: DataStream[Long] = env.generateSequence(0, 10)

  val iteratedStream = model.iterate(
    iteration => {
      val minusOne = iteration.connect(data).map(new FunctionModel)
//      val minusOne = iteration.map( v => v - 1)
      val newModel = minusOne.filter (_ >= 0)
//      val lessThanZero = minusOne.filter(_ <= 0)
      (newModel, newModel)
    }
  )
    iteratedStream.print()
//    iter.connect(data).map(new )

    env.execute("model train")
  }
}
