package uwaterloo
import java.nio.file.Files
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.io.Source
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection.mutable

/**
 * Hello world!
 *
 */
object App {
  def main(args: Array[String]): Unit ={
    val appName = "StreamingExample"
    val conf = new SparkConf().setAppName(appName).setMaster("local[1]")
    val batchDuration = Seconds(1)
    val ssc = new StreamingContext(conf,batchDuration)
    val checkpointDir = Files.createTempDirectory(appName).toString
    ssc.checkpoint(checkpointDir)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val lines = mutable.Queue[RDD[String]]()
    val dstream = ssc.queueStream(lines)
    val filename = "../data/targetdata_stream.orig"

    try{
      for (line <- Source.fromFile(filename).getLines()) {
        lines += ssc.sparkContext.makeRDD(Seq(line))
      }
    } catch {
      case ex: Exception => println("Bummer, an exception happened: " + ex.toString())
    }

    dstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
