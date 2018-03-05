package generation

import org.apache.flink.api.java.utils.ParameterTool


object CreateSourceExample {
  def main(args: Array[String]): Unit ={
    val params = ParameterTool.fromArgs(args)
    val readPath = if (params.has("readPath")) params.get("readPath") else "/Users/data/kddcup.data_10_percent_corrected_sample_train.csv"
    val writePath = if (params.has("writePath")) params.get("writePath") else "/Users/data/new/new"
    val intervalMills = if (params.has("intervalMills")) params.getInt("intervalMills") else 60000
    val numPoints = if (params.has("numPoints")) params.getInt("numPoints") else 10000

    UpdatedFile.run(readPath, writePath, intervalMills, numPoints)

  }
}
