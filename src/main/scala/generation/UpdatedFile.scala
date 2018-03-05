package generation

import scala.io.Source
import java.io.{File, BufferedWriter, FileWriter}


object UpdatedFile {
  /**
    * For each intervalMills, read numPoints number of lines from readPath
    * and write them in writePath.
    *
    * @param readPath
    * @param writePath
    * @param intervalMills
    * @param numPoints
    */
  def run(readPath: String, writePath: String, intervalMills: Long, numPoints: Long): Unit = {
    val s = Source.fromFile(new File(readPath)).getLines()
    var count = 0
    while (s.hasNext) {
      val startTime = System.currentTimeMillis()
      val pw = new BufferedWriter(new FileWriter(new File(writePath + startTime.toString)))
      while (count < numPoints && s.hasNext) {
        pw.append(s.next()).write("\n")
        count += 1
      }
      count = 0
      pw.flush()
      pw.close()
      val endTime = System.currentTimeMillis()
      val sleepTime = intervalMills - (endTime - startTime)
      if (sleepTime > 0) {
        Thread.sleep(sleepTime)
      }
    }
  }

}
